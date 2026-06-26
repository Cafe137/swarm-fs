import { existsSync, readFileSync, writeFileSync } from 'node:fs'

const BUCKET_COUNT = 65536

export class SlotMap {
    private data: Buffer
    private bytesPerBucket: number

    constructor(private path: string, depth: number) {
        const slotsPerBucket = 1 << (depth - 16)
        this.bytesPerBucket = slotsPerBucket / 8
        if (existsSync(path)) {
            this.data = readFileSync(path)
        } else {
            this.data = Buffer.alloc(BUCKET_COUNT * this.bytesPerBucket)
            writeFileSync(path, this.data)
        }
    }

    allocSlot(bucket: number): number {
        const base = bucket * this.bytesPerBucket
        for (let i = 0; i < this.bytesPerBucket; i++) {
            const byte = this.data[base + i]
            if (byte === 0xff) continue
            for (let bit = 0; bit < 8; bit++) {
                if ((byte & (1 << bit)) === 0) {
                    this.data[base + i] |= 1 << bit
                    return i * 8 + bit
                }
            }
        }
        throw new Error(`Bucket 0x${bucket.toString(16).padStart(4, '0')} is full`)
    }

    freeSlot(bucket: number, slot: number): void {
        const base = bucket * this.bytesPerBucket
        this.data[base + Math.floor(slot / 8)] &= ~(1 << slot % 8)
    }

    getStats() {
        const slotsPerBucket = this.bytesPerBucket * 8
        const totalSlots = BUCKET_COUNT * slotsPerBucket
        let occupiedSlots = 0
        let mostUtilizedBucket = 0
        let mostUtilizedCount = 0
        for (let bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            const base = bucket * this.bytesPerBucket
            let bucketOccupied = 0
            for (let i = 0; i < this.bytesPerBucket; i++) {
                let byte = this.data[base + i]
                while (byte) {
                    bucketOccupied += byte & 1
                    byte >>= 1
                }
            }
            occupiedSlots += bucketOccupied
            if (bucketOccupied > mostUtilizedCount) {
                mostUtilizedCount = bucketOccupied
                mostUtilizedBucket = bucket
            }
        }
        return {
            totalSlots,
            occupiedSlots,
            freeSlots: totalSlots - occupiedSlots,
            slotsPerBucket,
            mostUtilizedBucket,
            mostUtilizedCount
        }
    }

    save(): void {
        writeFileSync(this.path, this.data)
    }
}
