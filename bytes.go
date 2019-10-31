package bplustree

func Int64ToBytes(i int64) []byte {
	b := make([]byte, 8)

	b[7] = byte(i)
	b[6] = byte(i >> 8)
	b[5] = byte(i >> 16)
	b[4] = byte(i >> 24)
	b[3] = byte(i >> 32)
	b[2] = byte(i >> 40)
	b[1] = byte(i >> 48)
	b[0] = byte(i >> 56)

	return b
}

func Int32ToBytes(i int32) []byte {
	b := make([]byte, 4)

	b[3] = byte(i)
	b[2] = byte(i >> 8)
	b[1] = byte(i >> 16)
	b[0] = byte(i >> 24)

	return b
}
