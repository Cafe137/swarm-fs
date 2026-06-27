#!/usr/bin/env node
import { Binary, Types } from 'cafe-utility'
import { homedir } from 'node:os'
import { join, resolve } from 'node:path'
import { argv, env, loadEnvFile } from 'node:process'
import { benchSign, benchSplit, deleteFile, list, status, upload } from './commands.js'

main()

async function main() {
    try {
        loadEnvFile()
    } catch {
        // No .env file found, continue with environment variables
    }

    const command = Types.asString(argv[2])
    const stateDir = join(homedir(), '.swarmfs')

    if (command === 'status') {
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
        const batchDepth = Types.asNumber(env.SWARMFS_BATCH_DEPTH)
        const { totalSlots, occupiedSlots, freeSlots, slotsPerBucket, mostUtilizedBucket, mostUtilizedCount } = status({
            batchId,
            batchDepth,
            stateDir
        })
        console.log(`Slots: ${occupiedSlots} occupied, ${freeSlots} free, ${totalSlots} total`)
        console.log(
            `Most utilized bucket: 0x${mostUtilizedBucket
                .toString(16)
                .padStart(4, '0')} (${mostUtilizedCount}/${slotsPerBucket} slots occupied)`
        )
    } else if (command === 'list') {
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
        for (const { path, rootHash, kind, chunkCount } of list({ batchId, stateDir })) {
            console.log(`${Binary.uint8ArrayToHex(rootHash)}  ${path}  [${kind}]  ${chunkCount} chunks`)
        }
    } else if (command === 'upload') {
        const signer = Binary.uint256ToNumber(Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_SIGNER)), 'BE')
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
        const uploadUrl = Types.asString(env.SWARMFS_UPLOAD_URL)
        const batchDepth = Types.asNumber(env.SWARMFS_BATCH_DEPTH)
        const uploadArgs = argv.slice(3)
        const encrypt = uploadArgs.includes('--encrypt')
        const path = resolve(Types.asString(uploadArgs.find(a => !a.startsWith('--'))))
        let lastFile = ''
        const rootHash = await upload({
            signer,
            batchId,
            uploadUrl,
            batchDepth,
            path,
            stateDir,
            encrypt,
            onProgress: (file, chunks) => {
                if (file !== lastFile) {
                    if (lastFile) process.stderr.write('\n')
                    lastFile = file
                }
                process.stderr.write(`\r  ${file} — ${chunks} chunks`)
            }
        })
        if (lastFile) process.stderr.write('\n')
        console.log(Binary.uint8ArrayToHex(rootHash))
    } else if (command === 'delete') {
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
        const batchDepth = Types.asNumber(env.SWARMFS_BATCH_DEPTH)
        const rootHash = Binary.hexToUint8Array(Types.asHexString(argv[3]))
        await deleteFile({ batchId, batchDepth, rootHash, stateDir })
    } else if (command === 'bench:split') {
        const benchArgs = argv.slice(3)
        const encrypt = benchArgs.includes('--encrypt')
        const path = resolve(Types.asString(benchArgs.find(a => !a.startsWith('--'))))
        let lastFile = ''
        await benchSplit({
            path,
            encrypt,
            onProgress: (file, chunks) => {
                if (file !== lastFile) {
                    if (lastFile) process.stderr.write('\n')
                    lastFile = file
                }
                process.stderr.write(`\r  ${file} — ${chunks} chunks`)
            }
        })
        if (lastFile) process.stderr.write('\n')
    } else if (command === 'bench:sign') {
        const signer = Binary.uint256ToNumber(Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_SIGNER)), 'BE')
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
        const batchDepth = Types.asNumber(env.SWARMFS_BATCH_DEPTH)
        const benchArgs = argv.slice(3)
        const encrypt = benchArgs.includes('--encrypt')
        const path = resolve(Types.asString(benchArgs.find(a => !a.startsWith('--'))))
        let lastFile = ''
        await benchSign({
            signer,
            batchId,
            batchDepth,
            path,
            encrypt,
            onProgress: (file, chunks) => {
                if (file !== lastFile) {
                    if (lastFile) process.stderr.write('\n')
                    lastFile = file
                }
                process.stderr.write(`\r  ${file} — ${chunks} chunks`)
            }
        })
        if (lastFile) process.stderr.write('\n')
    } else {
        throw new Error(`Unknown command: ${command}. Use status, list, upload, delete, bench:split, or bench:sign.`)
    }
}
