package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/sys/unix"
)

const (
	XATTR_KEY = "ceph.file.layout.pool"
	SRC_POOL  = "cephfs.ibu.data_ec42"
	DST_POOL  = "cephfs.ibu.data_ec82"
	SCAN_FILE = "pool_scan.tab"
)

func main() {
	dryRun := pflag.Bool("dry-run", false, "Perform dry run without making changes")
	verbose := pflag.Bool("verbose", false, "Show verbose output")
	pflag.Parse()

	if len(pflag.Args()) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: migxattrs [-dry-run] [-verbose] CEPH_ROOT_DIR\n")
		os.Exit(1)
	}

	cephRoot := pflag.Arg(0)
	scanPath := filepath.Join(cephRoot, SCAN_FILE)

	fmt.Printf("Starting migration from %s to %s\nUsing scan file: %s\n", SRC_POOL, DST_POOL, scanPath)
	if *dryRun {
		fmt.Println("DRY RUN MODE - No changes will be made")
	}

	poolStats, err := analyzePoolScan(scanPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error analyzing scan file: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nSanity check - Pool distribution:")
	for pool, count := range poolStats {
		if pool == SRC_POOL {
			fmt.Printf("Files in %s (source): %d\n", pool, count)
		} else if pool == DST_POOL {
			fmt.Printf("Files in %s (destination): %d\n", pool, count)
		} else {
			fmt.Printf("Files in %s: %d\n", pool, count)
		}
	}

	if poolStats[SRC_POOL] == 0 {
		fmt.Println("\nNo files found in source pool. Nothing to migrate.")
		os.Exit(0)
	}

	fmt.Printf("\nProceeding with migration of %d files\n", poolStats[SRC_POOL])

	if !*dryRun {
		fmt.Print("Continue with migration? [y/N]: ")
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(strings.TrimSpace(response)) != "y" && strings.ToLower(strings.TrimSpace(response)) != "yes" {
			fmt.Println("Migration aborted.")
			os.Exit(0)
		}
	}

	total, migrated, errors := 0, 0, 0
	bytesTotal := int64(0)
	startTime := time.Now()

	file, err := os.Open(scanPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening scan file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	if *verbose {
		fmt.Println("Reading scan file...")
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)

	lineCount := 0
	lastProgressTime := time.Now()
	progressInterval := 5 * time.Second

	for scanner.Scan() {
		line := scanner.Text()
		lineCount++

		if *verbose && lineCount%10000 == 0 {
			fmt.Printf("Processed %d lines...\n", lineCount)
		} else if !*verbose && time.Since(lastProgressTime) > progressInterval {
			fmt.Printf("Processed %d lines...\r", lineCount)
			lastProgressTime = time.Now()
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		total++
		pool := fields[0]
		if pool != SRC_POOL {
			continue
		}

		absPath := filepath.Join(cephRoot, fields[1])
		info, err := os.Stat(absPath)
		if err != nil {
			if *verbose {
				fmt.Fprintf(os.Stderr, "Error accessing %s: %v\n", absPath, err)
			}
			errors++
			continue
		}

		if info.IsDir() {
			continue
		}

		currentPool, err := getXattr(absPath)
		if err != nil || string(currentPool) != SRC_POOL {
			if *verbose {
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error reading xattr for %s: %v\n", absPath, err)
				} else {
					fmt.Fprintf(os.Stderr, "Pool mismatch for %s: expected %s, got %s\n", absPath, SRC_POOL, string(currentPool))
				}
			}
			errors++
			continue
		}

		if *verbose {
			fmt.Printf("Migrating: %s (%.2f MB)\n", absPath, float64(info.Size())/(1024*1024))
		}

		if !*dryRun {
			if err := migrateFile(absPath, info); err != nil {
				fmt.Fprintf(os.Stderr, "Error migrating %s: %v\n", absPath, err)
				errors++
			} else {
				migrated++
				bytesTotal += info.Size()
				if *verbose && migrated%100 == 0 {
					fmt.Printf("Migrated %d files so far\n", migrated)
				}
			}
		} else {
			if *verbose {
				fmt.Printf("[DRY RUN] Would migrate: %s (%.2f MB)\n", absPath, float64(info.Size())/(1024*1024))
			}
			migrated++
			bytesTotal += info.Size()
		}
	}

	if !*verbose {
		fmt.Println()
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading scan file: %v\n", err)
	}

	elapsed := time.Since(startTime)
	fmt.Println("\nMigration Summary:")
	fmt.Printf("Lines processed:  %d\nFiles migrated:   %d\nBytes migrated:   %.2f MB\nErrors:           %d\nTime elapsed:     %v\n",
		lineCount, migrated, float64(bytesTotal)/(1024*1024), errors, elapsed)
	if *dryRun {
		fmt.Println("\nThis was a dry run. No changes were made.")
	}
}

func analyzePoolScan(scanPath string) (map[string]int, error) {
	if _, err := os.Stat(scanPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("scan file does not exist: %s", scanPath)
	}

	file, err := os.Open(scanPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	poolStats := make(map[string]int)
	scanner := bufio.NewScanner(file)
	lineCount := 0
	startTime := time.Now()

	fmt.Println("Analyzing pool distribution...")

	for scanner.Scan() {
		lineCount++
		if lineCount%100000 == 0 {
			fmt.Printf("Analyzed %d lines...\r", lineCount)
		}

		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 {
			poolStats[fields[0]]++
		}
	}

	fmt.Printf("Analyzed %d lines in %v\n", lineCount, time.Since(startTime))
	return poolStats, scanner.Err()
}

func getXattr(path string) ([]byte, error) {
	size, err := unix.Getxattr(path, XATTR_KEY, nil)
	if err != nil {
		return nil, err
	}

	value := make([]byte, size)
	_, err = unix.Getxattr(path, XATTR_KEY, value)
	return value, err
}

func migrateFile(path string, info os.FileInfo) error {
	tmpPath := path + ".mig"

	if tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY, info.Mode()); err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	} else {
		tmpFile.Close()
	}

	if err := unix.Setxattr(tmpPath, XATTR_KEY, []byte(DST_POOL), 0); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to set xattr: %w", err)
	}

	srcFile, err := os.Open(path)
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to open source file: %w", err)
	}

	dstFile, err := os.OpenFile(tmpPath, os.O_WRONLY, 0)
	if err != nil {
		srcFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to open temp file for writing: %w", err)
	}

	_, err = io.Copy(dstFile, srcFile)
	srcFile.Close()
	dstFile.Close()
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to copy data: %w", err)
	}

	if err := os.Chmod(tmpPath, info.Mode()); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to set permissions: %w", err)
	}

	if stat, ok := info.Sys().(*unix.Stat_t); ok {
		if err := os.Chown(tmpPath, int(stat.Uid), int(stat.Gid)); err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("failed to set ownership: %w", err)
		}
	}

	if err := os.Chtimes(tmpPath, time.Now(), info.ModTime()); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to set timestamps: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename: %w", err)
	}

	return nil
}
