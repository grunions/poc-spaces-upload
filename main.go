package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/miolini/datacounter"
	pb "gopkg.in/cheggaaa/pb.v1"

	"github.com/go-chi/chi"
	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
)

type S3Config struct {
	Key      string
	Secret   string
	Location string
	Bucket   string
	Endpoint string
	SSL      bool
}

type Config struct {
	S3 S3Config
}

var config *Config
var tpl *template.Template

func init() {
	config = &Config{}
	config.S3 = S3Config{
		Key:      os.Getenv("S3_KEY"),
		Secret:   os.Getenv("S3_SECRET"),
		Location: os.Getenv("S3_LOCATION"),
		Bucket:   os.Getenv("S3_BUCKET"),
		Endpoint: os.Getenv("S3_ENDPOINT"),
		SSL:      true,
	}

	tpl = template.Must(template.ParseFiles("upload.html"))

}

// Blob is a Remote gzip compressed object, which may either be a single file
// or a directory in a tar file
type Blob struct {
	IsDir            bool
	Size             int64
	UncompressedSize int64
	Hash             []byte

	// A human readable reference, for example a filename associated with the
	// blob, e.g. "Human Music.mp3". This is non-unique, user-controlled and
	// must not be used for any logic.
	Reference string
}

func index(w http.ResponseWriter, req *http.Request) {
	tpl.ExecuteTemplate(w, "upload.html", nil)
}

func Reader(client *minio.Client, hash []byte) (io.ReadCloser, error) {
	o, err := client.GetObject(config.S3.Bucket, fmt.Sprintf("blob/%x", hash), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return o, nil
}

func ReaderToBlob(fr io.Reader) (tmppath string, blob *Blob, e error) {

	tmpfile, err := ioutil.TempFile("", "upload")
	if err != nil {
		os.Remove(tmpfile.Name()) // try to clean up
		return "", nil, errors.Wrap(err, "Could not create Temporary file")
	}
	defer tmpfile.Close()

	cw := datacounter.NewWriterCounter(tmpfile)

	gw := gzip.NewWriter(cw)
	defer gw.Close()

	hw := sha256.New() // hash to generate a checksum

	// mw writes both to the gzip writer, as well as make a checksum
	mw := io.MultiWriter(hw, gw)

	// copy file reader into the chain
	written, err := io.Copy(mw, fr)
	if err != nil {
		os.Remove(tmpfile.Name()) // try to clean up
		return "", nil, errors.Wrap(err, "Error while processing")
	}

	gw.Close() // flush all remaining bytes
	blob = &Blob{
		IsDir:            false,
		Size:             int64(cw.Count()),
		UncompressedSize: written,
		Hash:             hw.Sum(nil),
	}

	return tmpfile.Name(), blob, nil
}

// CheckDuplicate return true if a duplicate exists
func CheckDuplicate(client *minio.Client, blob *Blob) bool {
	remoteFilename := fmt.Sprintf("blob/%x", blob.Hash)
	o, err := client.GetObject(config.S3.Bucket, remoteFilename, minio.GetObjectOptions{})
	if err != nil {
		return false

	}

	var info minio.ObjectInfo
	if info, err = o.Stat(); err != nil {
		return false
	}

	if blob.Size != info.Size {
		// size does not match
		return false
	}

	// found
	return true
}

func UploadBlob(client *minio.Client, path string, blob *Blob) error {
	remoteFilename := fmt.Sprintf("blob/%x", blob.Hash)

	bar := pb.New64(blob.Size)
	bar.ShowSpeed = true
	bar.ShowElapsedTime = true
	bar.ShowTimeLeft = true
	bar.Units = pb.U_BYTES
	bar.ShowFinalTime = true
	bar.Start()
	defer bar.Finish()

	written, err := client.FPutObject(
		config.S3.Bucket,
		remoteFilename,
		path,
		minio.PutObjectOptions{
			Progress:    bar,
			ContentType: "application/gzip",
			UserMetadata: map[string]string{
				"Uncompressed-Size": strconv.FormatInt(blob.UncompressedSize, 10),
				"Reference-Name":    blob.Reference,
				"Is-Dir":            strconv.FormatBool(blob.IsDir),
			},
		})
	bar.Set64(written)
	if err != nil {
		// try to remove
		client.RemoveObject(config.S3.Bucket, remoteFilename)
		return errors.Wrap(err, "Error while uploading blob")
	}

	return nil
}

func directoryUpload(w http.ResponseWriter, req *http.Request) {
	const name = "files"

	client, err := minio.New(
		config.S3.Endpoint,
		config.S3.Key,
		config.S3.Secret,
		config.S3.SSL)
	if err != nil {
		log.Printf("Error: %s", err)
		fmt.Fprintf(w, "Error\n")
		return
	}

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

	tmpfile, err := ioutil.TempFile("", "upload")
	if err != nil {
		fmt.Fprintln(w, "error opening archive")
		log.Printf("error opening archive: %s", err)
		return
	}
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	gw, err := gzip.NewWriterLevel(tmpfile, gzip.BestCompression)
	if err != nil {
		fmt.Fprintln(w, "error initializing compression")
		log.Printf("error initializing compression: %s", err)
		return
	}

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
			log.Print(err)
			return
		}

		if copied != file.Size {
			fmt.Fprintf(w,
				"WARNING: Filesize does not match amount uploaded!! (%d vs %d)",
				file.Size, copied)
			log.Print("Warning: filesize doesnt match!")
		}

		fmt.Fprintf(w, "Uploaded %10d byte: %s\n", copied, file.Filename)
		total += copied
		fr.Close()
	}

	tw.Close()
	gw.Close()
	tmpfile.Sync()

	filestat, err := tmpfile.Stat()
	if err != nil {
		panic(err)
	}
	filesize := filestat.Size()
	tmpfile.Close()

	blob := &Blob{
		IsDir:            true,
		Size:             filesize,
		UncompressedSize: total,
		Hash:             hw.Sum(nil),
		Reference:        "Dir",
	}

	if err := UploadBlob(client, tmpfile.Name(), blob); err != nil {
		panic(err)
	}

	fmt.Fprintf(w, "Upload completed. compressed %d bytes into %d (ratio of %0.3f)\n", total, filesize, (float64(filesize) / float64(total)))
	fmt.Fprintf(w, "Checksum: %x\n", hw.Sum(nil))
	return
}

func multiUpload(w http.ResponseWriter, req *http.Request) {
	const name = "files"

	client, err := minio.New(
		config.S3.Endpoint,
		config.S3.Key,
		config.S3.Secret,
		config.S3.SSL)
	if err != nil {
		log.Printf("Error: %s", err)
		fmt.Fprintf(w, "Error\n")
		return
	}

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

		tmppath, blob, err := ReaderToBlob(fr)
		if err != nil {
			fmt.Fprintln(w, "Error")
			log.Printf("Error: %s", err)
			return
		}
		defer os.Remove(tmppath)

		// set optional reference
		blob.Reference = file.Filename

		if CheckDuplicate(client, blob) {
			// file was already uploaded
			fmt.Fprintf(w, "replaced %10d byte: %s\n", blob.UncompressedSize, file.Filename)
			continue
		}

		err = UploadBlob(client, tmppath, blob)
		if err != nil {
			fmt.Fprintln(w, "Error")
			log.Printf("Error: %s", err)
			return
		}

		fmt.Fprintf(w, "Uploaded %10d byte: %s\n", blob.UncompressedSize, file.Filename)
	}
	fmt.Fprintln(w, "Upload completed")
	return
}

func main() {
	router := chi.NewMux()
	router.Get("/", index)
	router.Post("/upload-dir", directoryUpload)
	router.Post("/upload-files", multiUpload)
	http.ListenAndServe(":8000", router)
}
