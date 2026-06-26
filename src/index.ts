#!/usr/bin/env node
import { Binary, Chunk, ChunkSplitter, Dates, Types } from 'cafe-utility'
import { createReadStream, mkdirSync } from 'node:fs'
import { homedir } from 'node:os'
import { join, resolve } from 'node:path'
import { argv, env, loadEnvFile } from 'node:process'
import { ChunkRef, FileRegistry } from './registry.js'
import { SlotMap } from './slotmap.js'
import { stamp } from './stamper.js'

main()

function getSwarmfsPaths(batchId: Uint8Array) {
    const prefix = Binary.uint8ArrayToHex(batchId).slice(0, 8)
    const dir = join(homedir(), '.swarmfs')
    return { dir, free: join(dir, `swarmfs-${prefix}.free`), idx: join(dir, `swarmfs-${prefix}.idx`) }
}

async function main() {
    try {
        loadEnvFile()
    } catch {}

    const command = Types.asString(argv[2])

    if (command === 'status') {
        await status()
    } else if (command === 'list') {
        await list()
    } else if (command === 'upload') {
        await upload()
    } else if (command === 'delete') {
        await deleteFile()
    } else {
        throw new Error(`Unknown command: ${command}. Use status, list, upload, or delete.`)
    }
}

async function status() {
    const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
    const batchDepth = Types.asNumber(env.SWARMFS_BATCH_DEPTH)
    const { free } = getSwarmfsPaths(batchId)
    const slotMap = new SlotMap(free, batchDepth)
    const { totalSlots, occupiedSlots, freeSlots, slotsPerBucket, mostUtilizedBucket, mostUtilizedCount } =
        slotMap.getStats()
    console.log(`Slots: ${occupiedSlots} occupied, ${freeSlots} free, ${totalSlots} total`)
    console.log(
        `Most utilized bucket: 0x${mostUtilizedBucket
            .toString(16)
            .padStart(4, '0')} (${mostUtilizedCount}/${slotsPerBucket} slots occupied)`
    )
}

async function list() {
    const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
    const { idx } = getSwarmfsPaths(batchId)
    const registry = new FileRegistry(idx)
    for (const { path, rootHash } of registry.list()) {
        console.log(`${Binary.uint8ArrayToHex(rootHash)}  ${path}`)
    }
}

async function upload() {
    const signer = Binary.uint256ToNumber(Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_SIGNER)), 'BE')
    const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
    const uploadUrl = Types.asString(env.SWARMFS_UPLOAD_URL)
    const batchDepth = Types.asNumber(env.SWARMFS_BATCH_DEPTH)
    const filePath = resolve(Types.asString(argv[3]))

    const { dir, free, idx } = getSwarmfsPaths(batchId)
    mkdirSync(dir, { recursive: true })
    const slotMap = new SlotMap(free, batchDepth)
    const registry = new FileRegistry(idx)

    const chunks: ChunkRef[] = []
    const splitter = new ChunkSplitter(async (chunk: Chunk) => {
        const address = chunk.hash()
        const bucket = Binary.uint16ToNumber(address, 'BE')
        const slot = slotMap.allocSlot(bucket)
        chunks.push({ bucket, slot })
        const swarmPostageStamp = stamp(signer, batchId, chunk, slot)
        const response = await fetch(uploadUrl, {
            method: 'POST',
            body: Buffer.from(chunk.build()),
            headers: { 'swarm-postage-stamp': swarmPostageStamp },
            signal: AbortSignal.timeout(Dates.seconds(30))
        })
        if (!response.ok) {
            throw new Error(`Failed to upload chunk: ${response.status} ${response.statusText}`)
        }
    })

    const readStream = createReadStream(filePath)
    for await (const bytes of readStream) {
        console.log(`Read bytes: ${bytes.length} | Chunks processed: ${chunks.length}`)
        await splitter.append(bytes)
    }

    const rootChunk = await splitter.finalize()

    slotMap.save()
    registry.add(filePath, rootChunk.hash(), chunks)

    console.log(Binary.uint8ArrayToHex(rootChunk.hash()))
}

async function deleteFile() {
    const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
    const batchDepth = Types.asNumber(env.SWARMFS_BATCH_DEPTH)
    const rootHash = Binary.hexToUint8Array(Types.asHexString(argv[3]))

    const { free, idx } = getSwarmfsPaths(batchId)
    const slotMap = new SlotMap(free, batchDepth)
    const registry = new FileRegistry(idx)

    const chunks = registry.removeByRootHash(rootHash)
    if (!chunks) {
        throw new Error(`File not found: ${argv[3]}`)
    }

    for (const { bucket, slot } of chunks) {
        slotMap.freeSlot(bucket, slot)
    }

    slotMap.save()
}
