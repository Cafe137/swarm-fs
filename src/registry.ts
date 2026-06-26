import { existsSync, readFileSync, writeFileSync } from 'node:fs'

const ENCODER = new TextEncoder()
const DECODER = new TextDecoder()

export interface ChunkRef {
    bucket: number
    slot: number
}

interface FileEntry {
    path: string
    rootHash: Uint8Array
    chunks: ChunkRef[]
}

export class FileRegistry {
    private entries: FileEntry[]

    constructor(private path: string) {
        this.entries = existsSync(path) ? this.parse(readFileSync(path)) : []
    }

    list(): Array<{ path: string; rootHash: Uint8Array }> {
        return this.entries.map(({ path, rootHash }) => ({ path, rootHash }))
    }

    add(path: string, rootHash: Uint8Array, chunks: ChunkRef[]): void {
        this.entries.push({ path, rootHash, chunks })
        this.save()
    }

    removeByRootHash(rootHash: Uint8Array): ChunkRef[] | null {
        const hex = Buffer.from(rootHash).toString('hex')
        const i = this.entries.findIndex(e => Buffer.from(e.rootHash).toString('hex') === hex)
        if (i === -1) return null
        const [entry] = this.entries.splice(i, 1)
        this.save()
        return entry.chunks
    }

    private parse(buf: Buffer): FileEntry[] {
        const entries: FileEntry[] = []
        let offset = 0
        const fileCount = buf.readUInt32BE(offset)
        offset += 4
        for (let f = 0; f < fileCount; f++) {
            const pathLen = buf.readUInt16BE(offset)
            offset += 2
            const path = DECODER.decode(buf.subarray(offset, offset + pathLen))
            offset += pathLen
            const rootHash = buf.subarray(offset, offset + 32)
            offset += 32
            const chunkCount = buf.readUInt32BE(offset)
            offset += 4
            const chunks: ChunkRef[] = []
            for (let c = 0; c < chunkCount; c++) {
                const bucket = buf.readUInt16BE(offset)
                offset += 2
                const slot = buf.readUInt16BE(offset)
                offset += 2
                chunks.push({ bucket, slot })
            }
            entries.push({ path, rootHash, chunks })
        }
        return entries
    }

    private serialize(): Buffer {
        const parts: Buffer[] = []
        const fileCountBuf = Buffer.alloc(4)
        fileCountBuf.writeUInt32BE(this.entries.length)
        parts.push(fileCountBuf)
        for (const entry of this.entries) {
            const pathBytes = Buffer.from(ENCODER.encode(entry.path))
            const pathLenBuf = Buffer.alloc(2)
            pathLenBuf.writeUInt16BE(pathBytes.length)
            parts.push(pathLenBuf, pathBytes)
            parts.push(Buffer.from(entry.rootHash))
            const chunkCountBuf = Buffer.alloc(4)
            chunkCountBuf.writeUInt32BE(entry.chunks.length)
            parts.push(chunkCountBuf)
            for (const { bucket, slot } of entry.chunks) {
                const chunkBuf = Buffer.alloc(4)
                chunkBuf.writeUInt16BE(bucket)
                chunkBuf.writeUInt16BE(slot, 2)
                parts.push(chunkBuf)
            }
        }
        return Buffer.concat(parts)
    }

    private save(): void {
        writeFileSync(this.path, this.serialize())
    }
}
