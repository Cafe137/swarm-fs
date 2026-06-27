import { Binary, Chunk, Elliptic } from 'cafe-utility'

/**
 * "\x19Ethereum Signed Message:\n32"
 */
const SIGNATURE_PREFIX = new Uint8Array([
    25, 69, 116, 104, 101, 114, 101, 117, 109, 32, 83, 105, 103, 110, 101, 100, 32, 77, 101, 115, 115, 97, 103, 101, 58,
    10, 51, 50
])

/**
 *
 * @param signer Private key of the signer in bigint format
 * @param batchId Postage batch ID in Uint8Array format
 * @param chunk Chunk to be stamped
 * @param slot Slot number for the chunk
 * @returns A hex string representing the stamped data, which can be used in the "swarm-postage-stamp" header
 */
export function stamp(signer: bigint, batchId: Uint8Array, chunk: Chunk, slot: number, nowMs = Date.now()) {
    const address = chunk.hash()

    const bucket = Binary.uint16ToNumber(address, 'BE')
    const index = Binary.concatBytes(Binary.numberToUint32(bucket, 'BE'), Binary.numberToUint32(slot, 'BE'))

    const currentTimeNs = BigInt(nowMs) * 1_000_000n
    const timestamp = Binary.numberToUint64(currentTimeNs, 'BE')

    const data = Binary.concatBytes(address, batchId, index, timestamp)
    const digest = Binary.concatBytes(SIGNATURE_PREFIX, Binary.keccak256(data))
    const [r, s, v] = Elliptic.signMessage(digest, signer)
    const signature = Binary.concatBytes(
        Binary.numberToUint256(r, 'BE'),
        Binary.numberToUint256(s, 'BE'),
        new Uint8Array([Number(v)])
    )

    return Binary.uint8ArrayToHex(Binary.concatBytes(batchId, index, timestamp, signature))
}
