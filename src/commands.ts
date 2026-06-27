import { AsyncQueue, Binary, Chunk, ChunkSplitter, Dates } from 'cafe-utility'
import { createReadStream, mkdirSync, readdirSync, statSync, unlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { basename, join, relative, resolve } from 'node:path'
import { MantarayNode } from './manifest.js'
import { ChunkRef, EntryKind, FileRegistry } from './registry.js'
import { SlotMap } from './slotmap.js'
import { stamp } from './stamper.js'

const ENCODER = new TextEncoder()

type FetchFn = (url: string, init?: RequestInit) => Promise<Pick<Response, 'ok' | 'status' | 'statusText'>>

export interface UploadOpts {
    signer: bigint
    batchId: Uint8Array
    uploadUrl: string
    batchDepth: number
    path: string
    stateDir: string
    encrypt?: boolean
    fetchFn?: FetchFn
    onProgress?: (file: string, chunksProcessed: number) => void
}

export interface BenchSplitOpts {
    path: string
    encrypt?: boolean
    onProgress?: (file: string, chunksProcessed: number) => void
}

export interface BenchSignOpts {
    signer: bigint
    batchId: Uint8Array
    batchDepth: number
    path: string
    encrypt?: boolean
    onProgress?: (file: string, chunksProcessed: number) => void
}

export interface DeleteOpts {
    batchId: Uint8Array
    batchDepth: number
    rootHash: Uint8Array
    stateDir: string
}

export interface ListOpts {
    batchId: Uint8Array
    stateDir: string
}

export interface StatusOpts {
    batchId: Uint8Array
    batchDepth: number
    stateDir: string
}

function getPaths(stateDir: string, batchId: Uint8Array) {
    const prefix = Binary.uint8ArrayToHex(batchId).slice(0, 8)
    return {
        free: join(stateDir, `swarmfs-${prefix}.free`),
        idx: join(stateDir, `swarmfs-${prefix}.db`)
    }
}

export function buildChunkBody(chunk: Chunk, key?: Uint8Array): Uint8Array {
    if (key) {
        return Binary.concatBytes(
            Chunk.encryptSpan(key, Binary.numberToUint64(chunk.span, 'LE')),
            Chunk.encryptData(key, chunk.writer.buffer)
        )
    }
    return chunk.build()
}

function makeOnChunk(
    signer: bigint,
    batchId: Uint8Array,
    uploadUrl: string,
    fetchFn: FetchFn,
    slotMap: SlotMap,
    chunks: ChunkRef[],
    queue: AsyncQueue,
    uploadErrors: Error[]
): (chunk: Chunk, key?: Uint8Array) => Promise<void> {
    return async (chunk: Chunk, key?: Uint8Array) => {
        const address = key ? chunk.encryptedHash(key).address : chunk.hash()
        const bucket = Binary.uint16ToNumber(address, 'BE')
        const slot = slotMap.allocSlot(bucket)
        chunks.push({ bucket, slot })
        const swarmPostageStamp = stamp(signer, batchId, address, slot)
        const body = buildChunkBody(chunk, key)
        await queue.enqueue(async () => {
            try {
                const response = await fetchFn(uploadUrl, {
                    method: 'POST',
                    body: Buffer.from(body),
                    headers: { 'swarm-postage-stamp': swarmPostageStamp },
                    signal: AbortSignal.timeout(Dates.seconds(30))
                })
                if (!response.ok) {
                    uploadErrors.push(new Error(`Failed to upload chunk: ${response.status} ${response.statusText}`))
                }
            } catch (err) {
                uploadErrors.push(err instanceof Error ? err : new Error(String(err)))
            }
        })
    }
}

async function processPath(
    resolvedPath: string,
    encrypt: boolean,
    onChunk: (chunk: Chunk, key?: Uint8Array) => Promise<void>,
    setFile: (file: string) => void
): Promise<{ manifestRoot: Uint8Array; isDirectory: boolean }> {
    if (statSync(resolvedPath).isDirectory()) {
        // Pass 1: split all file content and collect hashes (32 or 64 bytes depending on encryption)
        const fileHashes = new Map<string, Uint8Array>()
        for (const filePath of walkDir(resolvedPath)) {
            const swarmPath = relative(resolvedPath, filePath)
            setFile(swarmPath)
            fileHashes.set(swarmPath, await splitFile(filePath, onChunk, encrypt))
        }

        // Pass 2: build the trie.
        // '/' is a standalone metadata fork at root key 47; file paths like 'index.html'
        // start at key 105 — completely different subtrees, no ordering conflict.
        const root = new MantarayNode({ encrypt })

        if (fileHashes.has('index.html')) {
            root.addFork(ENCODER.encode('/'), new Uint8Array(encrypt ? 64 : 32), {
                'website-index-document': 'index.html'
            })
        }

        for (const [swarmPath, hash] of fileHashes) {
            root.addFork(ENCODER.encode(swarmPath), hash, { 'Content-Type': guessMimeType(swarmPath) })
        }

        setFile('(manifest)')
        return { manifestRoot: await root.saveRecursively(onChunk), isDirectory: true }
    } else {
        setFile(basename(resolvedPath))
        const fileRef = await splitFile(resolvedPath, onChunk, encrypt)

        // Wrap in a manifest so the file is browseable via the Bzz gateway
        const filename = basename(resolvedPath)
        const root = new MantarayNode({ encrypt })
        setFile('(manifest)')
        root.addFork(ENCODER.encode('/'), new Uint8Array(encrypt ? 64 : 32), { 'website-index-document': filename })
        root.addFork(ENCODER.encode(filename), fileRef, { 'Content-Type': guessMimeType(resolvedPath) })
        return { manifestRoot: await root.saveRecursively(onChunk), isDirectory: false }
    }
}

export async function upload(opts: UploadOpts): Promise<Uint8Array> {
    const { signer, batchId, uploadUrl, batchDepth, stateDir } = opts
    const fetchFn: FetchFn = opts.fetchFn ?? fetch
    const resolvedPath = resolve(opts.path)

    mkdirSync(stateDir, { recursive: true })
    const { free, idx } = getPaths(stateDir, batchId)
    const slotMap = new SlotMap(free, batchDepth)
    const registry = new FileRegistry(idx)

    const chunks: ChunkRef[] = []
    const uploadErrors: Error[] = []
    const queue = new AsyncQueue(32, 128)
    const rawOnChunk = makeOnChunk(signer, batchId, uploadUrl, fetchFn, slotMap, chunks, queue, uploadErrors)

    const encrypt = opts.encrypt ?? false

    let chunksProcessed = 0
    let currentFile = ''
    const setFile = (file: string) => {
        currentFile = file
        chunksProcessed = 0
    }
    const onChunk = async (chunk: Chunk, key?: Uint8Array) => {
        await rawOnChunk(chunk, key)
        opts.onProgress?.(currentFile, ++chunksProcessed)
    }

    const { manifestRoot, isDirectory } = await processPath(resolvedPath, encrypt, onChunk, setFile)
    await queue.drain()
    if (uploadErrors.length > 0) throw uploadErrors[0]
    slotMap.save()
    registry.add(resolvedPath, manifestRoot, chunks, isDirectory ? 'manifest' : undefined)

    return manifestRoot
}

export async function benchSplit(opts: BenchSplitOpts): Promise<void> {
    const resolvedPath = resolve(opts.path)
    const encrypt = opts.encrypt ?? false

    let chunksProcessed = 0
    let currentFile = ''
    const setFile = (file: string) => {
        currentFile = file
        chunksProcessed = 0
    }
    const onChunk = async (_chunk: Chunk, _key?: Uint8Array) => {
        opts.onProgress?.(currentFile, ++chunksProcessed)
    }

    await processPath(resolvedPath, encrypt, onChunk, setFile)
}

export async function benchSign(opts: BenchSignOpts): Promise<void> {
    const { signer, batchId, batchDepth } = opts
    const resolvedPath = resolve(opts.path)
    const encrypt = opts.encrypt ?? false

    const tmpFree = join(tmpdir(), `swarmfs-bench-${process.pid}.free`)
    const slotMap = new SlotMap(tmpFree, batchDepth)

    let chunksProcessed = 0
    let currentFile = ''
    const setFile = (file: string) => {
        currentFile = file
        chunksProcessed = 0
    }
    const onChunk = async (chunk: Chunk, key?: Uint8Array) => {
        const address = key ? chunk.encryptedHash(key).address : chunk.hash()
        const bucket = Binary.uint16ToNumber(address, 'BE')
        const slot = slotMap.allocSlot(bucket)
        stamp(signer, batchId, address, slot)
        opts.onProgress?.(currentFile, ++chunksProcessed)
    }

    try {
        await processPath(resolvedPath, encrypt, onChunk, setFile)
    } finally {
        try {
            unlinkSync(tmpFree)
        } catch {}
    }
}

export async function splitFile(
    filePath: string,
    onChunk: (chunk: Chunk, key?: Uint8Array) => Promise<void>,
    encrypt: boolean
): Promise<Uint8Array> {
    let encryptedRef: { address: Uint8Array; key: Uint8Array } | undefined
    const splitterOnChunk = encrypt
        ? async (chunk: Chunk, key?: Uint8Array) => {
              if (key) encryptedRef = chunk.encryptedHash(key)
              await onChunk(chunk, key)
          }
        : onChunk
    const splitter = new ChunkSplitter(splitterOnChunk, encrypt)
    const readStream = createReadStream(filePath)
    for await (const bytes of readStream) {
        await splitter.append(bytes)
    }
    const root = await splitter.finalize()
    if (encrypt) {
        if (!encryptedRef) {
            throw new Error('Encrypted ChunkSplitter did not provide an encryption key')
        }
        return Binary.concatBytes(encryptedRef.address, encryptedRef.key)
    }
    return root.hash()
}

export async function deleteFile(opts: DeleteOpts): Promise<void> {
    const { batchId, batchDepth, rootHash, stateDir } = opts
    const { free, idx } = getPaths(stateDir, batchId)
    const slotMap = new SlotMap(free, batchDepth)
    const registry = new FileRegistry(idx)

    const chunks = registry.removeByRootHash(rootHash)
    if (!chunks) {
        throw new Error(`File not found: ${Binary.uint8ArrayToHex(rootHash)}`)
    }

    for (const { bucket, slot } of chunks) {
        slotMap.freeSlot(bucket, slot)
    }
    slotMap.save()
}

export function list(
    opts: ListOpts
): Array<{ path: string; rootHash: Uint8Array; kind: EntryKind; chunkCount: number }> {
    const { idx } = getPaths(opts.stateDir, opts.batchId)
    return new FileRegistry(idx).list()
}

export function status(opts: StatusOpts) {
    const { free } = getPaths(opts.stateDir, opts.batchId)
    return new SlotMap(free, opts.batchDepth).getStats()
}

function walkDir(dir: string): string[] {
    const files: string[] = []
    for (const entry of readdirSync(dir, { withFileTypes: true, recursive: true })) {
        if (entry.isFile()) {
            files.push(join(entry.parentPath, entry.name))
        }
    }
    return files
}

function guessMimeType(filePath: string): string {
    const ext = filePath.slice(filePath.lastIndexOf('.')).toLowerCase()
    const types: Record<string, string> = {
        '.html': 'text/html',
        '.htm': 'text/html',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.mjs': 'application/javascript',
        '.json': 'application/json',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml',
        '.ico': 'image/x-icon',
        '.webp': 'image/webp',
        '.woff': 'font/woff',
        '.woff2': 'font/woff2',
        '.ttf': 'font/ttf',
        '.txt': 'text/plain',
        '.xml': 'application/xml',
        '.pdf': 'application/pdf',
        '.mp4': 'video/mp4',
        '.webm': 'video/webm',
        '.mp3': 'audio/mpeg',
        '.wav': 'audio/wav'
    }
    return types[ext] ?? 'application/octet-stream'
}
