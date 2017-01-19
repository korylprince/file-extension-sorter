package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

//File represents a file with its hash
type File struct {
	FullPath  string
	Name      string
	Extension string
	Hash      string
}

//MD5Sum returns the MD5 hash of the given file, or an error if one occured
func MD5Sum(filePath string) (string, error) {
	//open file
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("Could not open file: %v", err)
	}
	defer file.Close()

	//copy data into hash
	hash := md5.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		return "", fmt.Errorf("Could not hash file: %v", err)
	}

	//return sum
	return hex.EncodeToString(hash.Sum(nil)), nil
}

//Walker walks root and sends a *File on out for each file encountered
func Walker(inputPath string, out chan<- *File) error {
	total := 0

	//print total at end
	defer func() {
		log.Println("Total files found:", total)
	}()

	return filepath.Walk(inputPath, func(path string, info os.FileInfo, err error) error {
		//only work on files
		if !info.Mode().IsRegular() {
			return nil
		}

		total++

		//get absolute path
		abs, err := filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("Could not get absolute path of %s: %v", path, err)
		}

		//create and send file
		n := info.Name()
		f := &File{FullPath: abs, Name: n, Extension: filepath.Ext(n)}
		if err != nil {
			return fmt.Errorf("Could not walk path %s: %v", f.FullPath, err)
		}
		out <- f

		return nil
	})

}

//Hasher computes the hash for each incoming *File and sends it out
func Hasher(wg *sync.WaitGroup, in <-chan *File, out chan<- *File) {
	defer wg.Done()
	for f := range in {
		//compute hash
		hash, err := MD5Sum(f.FullPath)
		if err != nil {
			log.Printf("Warning skipping file: Could not hash %s: %v\n", f.FullPath, err)
		}
		f.Hash = hash

		//send
		out <- f
	}
}

//CopyFile copies the file from src to dest, returning an error if one occured
func CopyFile(src, dest string) error {
	//open file
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("Could not open source file %s: %v", src, err)
	}
	defer in.Close()

	out, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("Could not open destination file %s: %v", dest, err)
	}

	_, err = io.Copy(out, in)
	if err != nil {
		return fmt.Errorf("Could not copy data from %s to %s: %v", src, dest, err)
	}

	err = out.Close()
	if err != nil {
		return fmt.Errorf("Could not close destination file %s: %v", dest, err)
	}

	return nil
}

//Copier receives *Files from in and copies the underlying file to outputDir, deduped and organized by file extension
func Copier(wg *sync.WaitGroup, in <-chan *File, root string) {
	defer wg.Done()

	total := 0

	hashes := map[string]struct{}{}
	extensions := map[string]struct{}{}

	for f := range in {
		//skip file if hash has already been seen
		if _, ok := hashes[f.Hash]; ok {
			continue
		}

		//format extension
		ext := f.Extension
		if len(ext) > 0 && ext[0] == '.' {
			ext = ext[1:]
		} else {
			ext = "no-extension"
		}

		if ext == "" {
			ext = "no-extension"
		}

		outputDir := filepath.Join(root, ext)
		outputFile := filepath.Join(outputDir, f.Name)

		//create extension directory if it does not exist
		if _, ok := extensions[f.Extension]; !ok {
			err := os.MkdirAll(outputDir, 0755)
			//stop if directory can't be created
			if err != nil {
				log.Fatalf("Fatal Error: Could not create directory %s: %v\n", outputDir, err)
			}
			extensions[f.Extension] = struct{}{}
		}

		//copy file
		err := CopyFile(f.FullPath, outputFile)
		if err != nil {
			log.Printf("Warning skipping file: Could copy file %s: %v\n", f.FullPath, err)
		}

		//add hash to dedup set
		hashes[f.Hash] = struct{}{}

		total++
	}

	log.Println("Total files copied:", total)
}

func main() {
	var in string
	var out string

	//parse flags
	flag.StringVar(&in, "in", "", "Input directory")
	flag.StringVar(&out, "out", "", "Output directory")
	workers := flag.Int("workers", 10, "Number of hash workers. More is not always better")

	flag.Parse()

	//verify file paths
	if in == "" {
		flag.Usage()
		return
	}

	inputPath, err := filepath.Abs(in)
	if err != nil {
		fmt.Printf("Invalid input path %s: %v\n", in, err)
		flag.Usage()
		return
	}

	if out == "" {
		flag.Usage()
		return
	}

	outputPath, err := filepath.Abs(out)
	if err != nil {
		fmt.Printf("Invalid output path %s: %v\n", out, err)
		flag.Usage()
		return
	}

	//set up sync primatives
	wg := new(sync.WaitGroup)
	wg.Add(*workers)

	copyWG := new(sync.WaitGroup)
	copyWG.Add(1)

	walk := make(chan *File, 1024)
	copy := make(chan *File, 1024)

	//start goroutines
	for i := 0; i < *workers; i++ {
		go Hasher(wg, walk, copy)
	}

	go Copier(copyWG, copy, outputPath)

	err = Walker(inputPath, walk)
	if err != nil {
		log.Fatalf("Fatal Error: Error walking input path %s: %v\n", inputPath, err)
	}

	close(walk)
	wg.Wait()
	close(copy)
	copyWG.Wait()
}
