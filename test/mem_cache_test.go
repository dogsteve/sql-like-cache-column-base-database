package test

import (
	"a-eighty/mem_cache/data_query"
	"a-eighty/mem_cache/map_table"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func doTestMemCache(t *testing.T) {
	map_table.InitDataBase()
	map_table.CreateDatabase("test")
	map_table.CreateTable("test", "employees")
	sqlSession := data_query.SqlSession{
		DatabaseName: "test",
	}

	fmt.Println("--- Starting INSERT benchmark ---")
	var memStatsBefore, memStatsAfter runtime.MemStats

	insertProfileFile, err := os.Create("insert_cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer insertProfileFile.Close()
	if err := pprof.StartCPUProfile(insertProfileFile); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}

	runtime.ReadMemStats(&memStatsBefore)
	insertStartTime := time.Now()

	sql := "INSERT INTO employees (id, name, department, hire_date) VALUES (%d, 'John Doe', 'Engineering', '2023-01-15');"

	for i := 0; i < 5000; i++ {
		formatedSql := fmt.Sprintf(sql, i)
		sqlSession.ExecuteSQL(formatedSql)
	}

	insertDuration := time.Since(insertStartTime)
	pprof.StopCPUProfile()
	runtime.ReadMemStats(&memStatsAfter)

	fmt.Printf("INSERT operation took: %s\n", insertDuration)
	fmt.Println("Memory usage during INSERT:")
	fmt.Printf("  Total Allocated: %v MiB\n", bToMb(memStatsAfter.TotalAlloc-memStatsBefore.TotalAlloc))
	fmt.Printf("  Final Heap Alloc: %v MiB\n", bToMb(memStatsAfter.Alloc))
	fmt.Println("CPU profile saved to insert_cpu.prof")
	fmt.Println("---------------------------------")
	fmt.Println()

	fmt.Println("--- Starting SELECT benchmark ---")

	selectProfileFile, err := os.Create("select_cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer selectProfileFile.Close()
	if err := pprof.StartCPUProfile(selectProfileFile); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}

	runtime.ReadMemStats(&memStatsBefore)
	selectStartTime := time.Now()

	selectSql := "select * from employees where id > 50"
	rs, _ := sqlSession.ExecuteSQL(selectSql)
	fmt.Printf("total rows %d\n", len(rs.Rows))

	selectDuration := time.Since(selectStartTime)
	pprof.StopCPUProfile()
	runtime.ReadMemStats(&memStatsAfter)

	fmt.Printf("SELECT operation took: %s\n", selectDuration)
	fmt.Println("Memory usage during SELECT:")
	fmt.Printf("  Total Allocated: %v MiB\n", bToMb(memStatsAfter.TotalAlloc-memStatsBefore.TotalAlloc))
	fmt.Printf("  Final Heap Alloc: %v MiB\n", bToMb(memStatsAfter.Alloc))
	fmt.Println("CPU profile saved to select_cpu.prof")
	fmt.Println("---------------------------------")
}

func TestMemCache(t *testing.T) {
	for i := 0; i < 10; i++ {
		doTestMemCache(t)
	}
}
