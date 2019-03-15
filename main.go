package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"hash"
	"html/template"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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

// Blob is a gzip compressed object, which may either be a single file
// or a directory in a tar file
type Blob struct {
	IsDir bool

	File *os.File

	gw  io.WriteCloser             // gzip writer for compression
	hw  hash.Hash                  // hashwriter for checksum
	ccw *datacounter.WriterCounter // countWriter for counting written compressed bytes
	ucw *datacounter.WriterCounter // countWriter for counting written uncompressed bytes
	mw  io.Writer                  // multiWriter for combining hash and gzip

	// A human readable reference, for example a filename associated with the
	// blob, e.g. "Human Music.mp3". This is non-unique, user-controlled and
	// must not be used for any logic.
	Reference string
}

func NewBlob() (*Blob, error) {
	blob := &Blob{
		IsDir: false,
	}

	var err error

	blob.File, err = ioutil.TempFile("", "blob")
	if err != nil {
		return nil, errors.Wrap(err, "Blob: could not create temporary file")
	}

	blob.ccw = datacounter.NewWriterCounter(blob.File)
	blob.gw, _ = gzip.NewWriterLevel(blob.ccw, gzip.BestCompression)
	blob.ucw = datacounter.NewWriterCounter(blob.gw)
	blob.hw = sha256.New()
	blob.mw = io.MultiWriter(blob.ucw, blob.hw)

	return blob, nil
}

// Close finishes the writing process to the blob
func (blob *Blob) Close() error {
	blob.gw.Close()
	return blob.File.Close()
}

// Size returns the Compressed blob size
func (blob *Blob) Size() int64 {
	return int64(blob.ccw.Count())
}

// UncompressedSize returns the original size, or the size of the
// Tar file if the blob is a dir blob
func (blob *Blob) UncompressedSize() int64 {
	return int64(blob.ucw.Count())
}

// Hash returns the checksum of the uncompressed data
func (blob *Blob) Hash() []byte {
	return blob.hw.Sum(nil)
}

// Write implements the standard Write interface
func (blob *Blob) Write(b []byte) (n int, err error) {
	return blob.mw.Write(b)
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

func ReaderToBlob(fr io.Reader) (blob *Blob, e error) {

	blob, err := NewBlob()
	if err != nil {
		blob.Close()
		os.Remove(blob.File.Name()) // try to clean up
		return nil, errors.Wrap(err, "Could not create blob")
	}
	defer blob.Close()

	// copy file reader into the chain
	_, err = io.Copy(blob, fr)
	if err != nil {
		os.Remove(blob.File.Name()) // try to clean up
		return nil, errors.Wrap(err, "Error while processing")
	}

	blob.Close() // flush all remaining bytes

	return blob, nil
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

	if blob.Size() != info.Size {
		// size does not match
		return false
	}

	// found
	return true
}

func UploadBlob(client *minio.Client, blob *Blob) error {
	remoteFilename := fmt.Sprintf("blob/%x", blob.Hash())

	bar := pb.New64(blob.Size())
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
		blob.File.Name(),
		minio.PutObjectOptions{
			Progress:    bar,
			ContentType: "application/gzip",
			UserMetadata: map[string]string{
				"Uncompressed-Size": strconv.FormatInt(blob.UncompressedSize(), 10),
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

func TarDir(src string, writer io.Writer) error {
	// ensure the src actually exists before trying to tar it
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("Unable to tar files - %v", err.Error())
	}

	tw := tar.NewWriter(writer)
	defer tw.Close()

	// reusable buffer for io.CopyBuffer
	copyBuffer := make([]byte, 32*1024)

	// walk path
	return filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {

		// return on any error
		if err != nil {
			return err
		}

		// create a new dir/file header
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}

		// reset modification time, to make output deterministic
		header.ModTime = time.Time{}

		// update the name to correctly reflect the desired destination when untaring
		header.Name = strings.TrimPrefix(strings.Replace(file, src, "", -1), string(filepath.Separator))

		// write the header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// return on non-regular files for this suggested update)
		if !fi.Mode().IsRegular() {
			return nil
		}

		// open files for taring
		f, err := os.Open(file)
		if err != nil {
			return err
		}

		// copy file data into tar writer
		if _, err := io.CopyBuffer(tw, f, copyBuffer); err != nil {
			return err
		}

		// manually close here after each file operation; defering would cause each file close
		// to wait until all operations have completed.
		f.Close()

		return nil
	})

}

// Untargz takes a destination path and a reader; a tar reader loops over the tarfile
// creating the file structure at 'dst' along the way, and writing any files
func Untargz(dst string, r io.Reader) error {

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	// reusable buffer for io.CopyBuffer
	copyBuffer := make([]byte, 32*1024)

	for {
		header, err := tr.Next()

		switch {

		// if no more files are found return
		case err == io.EOF:
			return nil

		// return any other error
		case err != nil:
			return err

		// if the header is nil, just skip it (not sure how this happens)
		case header == nil:
			continue
		}

		// the target location where the dir/file should be created
		target := filepath.Join(dst, header.Name)

		// the following switch could also be done using fi.Mode(), not sure if there
		// a benefit of using one vs. the other.
		// fi := header.FileInfo()

		// check the file type
		switch header.Typeflag {

		// if its a dir and it doesn't exist create it
		case tar.TypeDir:
			if _, err := os.Stat(target); err != nil {
				if err := os.MkdirAll(target, 0755); err != nil {
					return err
				}
			}

		// if it's a file create it
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}

			// copy over contents
			if _, err := io.CopyBuffer(f, tr, copyBuffer); err != nil {
				return err
			}

			// manually close here after each file operation; defering would cause each file close
			// to wait until all operations have completed.
			f.Close()

		case tar.TypeSymlink:
			if err := os.Symlink(header.Linkname, target); err != nil {
				return err
			}

		default:
			log.Print("Tar: ignoring unknown tar header")
		}
	}
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

	blob, err := NewBlob()
	if err != nil {
		fmt.Fprintln(w, "error opening archive")
		log.Printf("error creating blob: %s", err)
		return
	}
	blob.IsDir = true
	defer os.Remove(blob.File.Name())
	defer blob.Close()

	tw := tar.NewWriter(blob)
	defer tw.Close()

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

		_, err = io.Copy(tw, fr)
		if err != nil {
			fmt.Fprintln(w, err)
			log.Print(err)
			return
		}

		fmt.Fprintf(w, "Uploaded %s\n", file.Filename)
		fr.Close()
	}

	tw.Close() // flush remaining bytes
	blob.Close()

	if err := UploadBlob(client, blob); err != nil {
		panic(err)
	}

	fmt.Fprintf(w, "Upload completed. compressed %d bytes into %d (ratio of %0.3f)\n",
		blob.UncompressedSize(), blob.Size(),
		(float64(blob.Size()) / float64(blob.UncompressedSize())))
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

		blob, err := ReaderToBlob(fr)
		if err != nil {
			fmt.Fprintln(w, "Error")
			log.Printf("Error: %s", err)
			return
		}
		defer os.Remove(blob.File.Name())

		// set optional reference
		blob.Reference = file.Filename

		if CheckDuplicate(client, blob) {
			// file was already uploaded
			fmt.Fprintf(w, "replaced %10d byte: %s\n", blob.UncompressedSize, file.Filename)
			continue
		}

		err = UploadBlob(client, blob)
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
