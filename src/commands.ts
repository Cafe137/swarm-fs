import { Binary, Chunk, ChunkSplitter, Dates } from 'cafe-utility'
import { createReadStream, mkdirSync, readdirSync, statSync } from 'node:fs'
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
    fetchFn?: FetchFn
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

function makeOnChunk(
    signer: bigint,
    batchId: Uint8Array,
    uploadUrl: string,
    fetchFn: FetchFn,
    slotMap: SlotMap,
    chunks: ChunkRef[]
): (chunk: Chunk) => Promise<void> {
    return async (chunk: Chunk) => {
        const address = chunk.hash()
        const bucket = Binary.uint16ToNumber(address, 'BE')
        const slot = slotMap.allocSlot(bucket)
        chunks.push({ bucket, slot })
        const swarmPostageStamp = stamp(signer, batchId, chunk, slot)
        const response = await fetchFn(uploadUrl, {
            method: 'POST',
            body: Buffer.from(chunk.build()),
            headers: { 'swarm-postage-stamp': swarmPostageStamp },
            signal: AbortSignal.timeout(Dates.seconds(30))
        })
        if (!response.ok) {
            throw new Error(`Failed to upload chunk: ${response.status} ${response.statusText}`)
        }
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
    const rawOnChunk = makeOnChunk(signer, batchId, uploadUrl, fetchFn, slotMap, chunks)

    let chunksProcessed = 0
    let currentFile = ''
    const setFile = (file: string) => {
        currentFile = file
        chunksProcessed = 0
    }
    const onChunk = async (chunk: Chunk) => {
        await rawOnChunk(chunk)
        opts.onProgress?.(currentFile, ++chunksProcessed)
    }

    let manifestRoot: Uint8Array

    if (statSync(resolvedPath).isDirectory()) {
        // Pass 1: upload all file content and collect hashes
        const fileHashes = new Map<string, Uint8Array>()
        for (const filePath of walkDir(resolvedPath)) {
            const swarmPath = relative(resolvedPath, filePath)
            setFile(swarmPath)
            const splitter = new ChunkSplitter(onChunk)
            const readStream = createReadStream(filePath)
            for await (const bytes of readStream) {
                await splitter.append(bytes)
            }
            fileHashes.set(swarmPath, (await splitter.finalize()).hash())
        }

        // Pass 2: build the trie.
        // '/' is a standalone metadata fork at root key 47; file paths like 'index.html'
        // start at key 105 — completely different subtrees, no ordering conflict.
        const root = new MantarayNode()

        if (fileHashes.has('index.html')) {
            root.addFork(ENCODER.encode('/'), new Uint8Array(32), { 'website-index-document': 'index.html' })
        }

        for (const [swarmPath, hash] of fileHashes) {
            root.addFork(ENCODER.encode(swarmPath), hash, { 'Content-Type': guessMimeType(swarmPath) })
        }

        setFile('(manifest)')
        manifestRoot = await root.saveRecursively(onChunk)
        slotMap.save()
        registry.add(resolvedPath, manifestRoot, chunks, 'manifest')
    } else {
        setFile(basename(resolvedPath))
        const splitter = new ChunkSplitter(onChunk)
        const readStream = createReadStream(resolvedPath)
        for await (const bytes of readStream) {
            await splitter.append(bytes)
        }

        const rootChunk = await splitter.finalize()

        // Wrap in a manifest so the file is browseable via the Bzz gateway
        const filename = basename(resolvedPath)
        const root = new MantarayNode()
        setFile('(manifest)')
        root.addFork(ENCODER.encode('/'), new Uint8Array(32), { 'website-index-document': filename })
        root.addFork(ENCODER.encode(filename), rootChunk.hash(), { 'Content-Type': guessMimeType(resolvedPath) })
        manifestRoot = await root.saveRecursively(onChunk)

        slotMap.save()
        registry.add(resolvedPath, manifestRoot, chunks)
    }

    return manifestRoot
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

export function list(opts: ListOpts): Array<{ path: string; rootHash: Uint8Array; kind: EntryKind }> {
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
