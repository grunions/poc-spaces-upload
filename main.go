package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/go-chi/chi"
)

func index(w http.ResponseWriter, req *http.Request) {
	file, err := os.Open("index.html")
	if err != nil {
		http.Error(w, "index not found", 404)
		return
	}

	io.Copy(w, file)
}

func directoryUpload(w http.ResponseWriter, req *http.Request) {
	const name = "files"

	// parse forms, storing everything thats bigger than
	// 30mb on disk.
	const size30mb = 30 << 20
	if err := req.ParseMultipartForm(size30mb); err != nil {
		fmt.Fprintln(w, err)
		return
	}

	form := req.MultipartForm
	defer form.RemoveAll()

	files, ok := form.File[name]
	if !ok {
		fmt.Fprintln(w, "no files supplied")
		return
	}

	fw, err := ioutil.TempFile("upload", "upload")
	if err != nil {
		fmt.Fprintln(w, "error opening archive")
		log.Printf("error opening archive: %s", err)
		return
	}
	defer fw.Close()

	gw := gzip.NewWriter(fw)
	defer gw.Close()
	hw := sha256.New() // hash to generate a checksum
	mw := io.MultiWriter(hw, gw)
	tw := tar.NewWriter(mw)
	defer tw.Close()

	var total int64

	for _, file := range files {
		fr, err := file.Open()
		if err != nil {
			continue
		}
		defer fr.Close()

		header := &tar.Header{
			Name: file.Filename,
			Mode: 0640,
			Size: file.Size,
		}
		if err := tw.WriteHeader(header); err != nil {
			fmt.Fprintf(w, "Error: %s", err)
			return
		}

		copied, err := io.Copy(tw, fr)
		if err != nil {
			fmt.Fprintln(w, err)
			return
		}

		if copied != file.Size {
			fmt.Fprintf(w,
				"WARNING: Filesize does not match amount uploaded!! (%d vs %d)",
				file.Size, copied)
		}

		fmt.Fprintf(w, "Uploaded %10d byte: %s\n", copied, file.Filename)
		total += copied
	}

	// rename file to be content-indexed
	os.Rename(fw.Name(), fmt.Sprintf("upload/%x", hw.Sum(nil)))

	filestat, _ := fw.Stat()
	filesize := filestat.Size()

	fmt.Fprintf(w, "Upload completed. compressed %d bytes into %d (ratio of %0.3f)\n", total, filesize, (float64(filesize) / float64(total)))
	fmt.Fprintf(w, "Checksum: %x\n", hw.Sum(nil))
	return
}

func multiUpload(w http.ResponseWriter, req *http.Request) {
	const name = "files"

	// parse forms, storing everything thats bigger than
	// 30mb on disk.
	if err := req.ParseMultipartForm(30 << 20); err != nil {
		fmt.Fprintln(w, err)
		return
	}

	form := req.MultipartForm
	defer form.RemoveAll()

	files, ok := form.File[name]
	if !ok {
		fmt.Fprintln(w, "no files supplied")
		return
	}

	for _, file := range files {
		fr, err := file.Open()
		if err != nil {
			continue
		}
		defer fr.Close()

		// resulting file path
		fullpath := filepath.Join("upload", file.Filename)

		// prepare dir
		os.MkdirAll(filepath.Dir(fullpath), 0755)

		fw, err := os.OpenFile(
			fullpath,
			os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			fmt.Fprintln(w, "error opening file")
			log.Printf("error opening file: %s", err)
			continue
		}
		defer fw.Close()

		copied, err := io.Copy(fw, fr)
		if err != nil {
			fmt.Fprintln(w, err)
			return
		}

		if copied != file.Size {
			fmt.Fprintf(w,
				"WARNING: Filesize does not match amount uploaded!! (%d vs %d)",
				file.Size, copied)
		}

		fmt.Fprintf(w, "Uploaded %10d byte: %s\n", copied, file.Filename)
	}
	fmt.Fprintln(w, "Upload completed")
	return
}

func main() {
	router := chi.NewMux()
	router.Get("/", index)
	router.Post("/", directoryUpload)
	http.ListenAndServe(":8000", router)
}
