package screening

import (
	"sync"
	"testing"
)

// TestDedupRaceCondition akan menyuruh 100 pekerja mengecek ID yang sama
// pada milidetik yang sama persis untuk menguji ketahanan Gembok (Mutex) Anda.
func TestDedupRaceCondition(t *testing.T) {
	// Buat buku catatan dengan TTL 60 detik
	cache := NewDedupCache(60)
	
	// Siapkan 100 pekerja
	var wg sync.WaitGroup
	jumlahPekerja := 100

	// Jalankan 100 Goroutine secara bersamaan
	for i := 0; i < jumlahPekerja; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Semua pekerja menyerang memori dengan mengecek ID yang sama
			cache.IsDuplicate("ID-BRUTAL-TEST-123")
		}()
	}

	// Tunggu semua pekerja selesai
	wg.Wait()

	// Jika kita sampai di baris ini dan aplikasi tidak crash, berarti Mutex Anda LULUS!
	t.Log("Ujian Race Condition berhasil dilewati dengan aman!")
}