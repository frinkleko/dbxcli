// Copyright © 2016 Dropbox, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/files"
	"github.com/dustin/go-humanize"
	"github.com/mitchellh/ioprogress"
	"github.com/spf13/cobra"
)

func downloadFile(dbx files.Client, entry *files.FileMetadata, dst string) error {
	arg := files.NewDownloadArg(entry.PathLower)
	res, contents, err := dbx.Download(arg)
	if err != nil {
		return err
	}
	defer contents.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()

	progressbar := &ioprogress.Reader{
		Reader: contents,
		DrawFunc: ioprogress.DrawTerminalf(os.Stderr, func(progress, total int64) string {
			return fmt.Sprintf("Downloading %s: %s/%s",
				entry.PathDisplay, humanize.IBytes(uint64(progress)), humanize.IBytes(uint64(total)))
		}),
		Size: int64(res.Size),
	}

	if _, err = io.Copy(f, progressbar); err != nil {
		return err
	}

	return nil
}

func downloadFolder(dbx files.Client, folder *files.FolderMetadata, dst string, workers int) error {
	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}
	arg := files.NewListFolderArg(folder.PathLower)
	arg.Recursive = true
	res, err := dbx.ListFolder(arg)
	if err != nil {
		return err
	}

	entries := res.Entries
	for res.HasMore {
		contArg := files.NewListFolderContinueArg(res.Cursor)
		res, err = dbx.ListFolderContinue(contArg)
		if err != nil {
			return err
		}
		entries = append(entries, res.Entries...)
	}

	jobCh := make(chan files.IsMetadata, len(entries))
	errCh := make(chan error, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for entry := range jobCh {
				switch f := entry.(type) {
				case *files.FileMetadata:
					rel := strings.TrimPrefix(f.PathLower, folder.PathLower)
					rel = strings.TrimPrefix(rel, "/")
					localPath := filepath.Join(dst, filepath.FromSlash(rel))
					if err := downloadFile(dbx, f, localPath); err != nil {
						errCh <- err
						return
					}
				case *files.FolderMetadata:
					rel := strings.TrimPrefix(f.PathLower, folder.PathLower)
					rel = strings.TrimPrefix(rel, "/")
					localPath := filepath.Join(dst, filepath.FromSlash(rel))
					if err := os.MkdirAll(localPath, 0755); err != nil {
						errCh <- err
						return
					}
				}
			}
		}()
	}

	for _, entry := range entries {
		jobCh <- entry
	}
	close(jobCh)

	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func get(cmd *cobra.Command, args []string) (err error) {
	if len(args) == 0 || len(args) > 2 {
		return errors.New("`get` requires `src` and/or `dst` arguments")
	}

	src, err := validatePath(args[0])
	if err != nil {
		return
	}

	// Default `dst` to the base segment of the source path; use the second argument if provided.
	dst := path.Base(src)
	if len(args) == 2 {
		dst = args[1]
	}
	// If `dst` is a directory, append the source filename.
	if info, err := os.Stat(dst); err == nil && info.IsDir() {
		dst = filepath.Join(dst, path.Base(src))
	}

	workers, err := cmd.Flags().GetInt("workers")
	if err != nil {
		return err
	}
	if workers < 1 {
		workers = 1
	}

	dbx := files.New(config)
	metaArg := files.NewGetMetadataArg(src)
	res, err := dbx.GetMetadata(metaArg)
	if err != nil {
		return err
	}

	switch f := res.(type) {
	case *files.FolderMetadata:
		return downloadFolder(dbx, f, dst, workers)
	case *files.FileMetadata:
		return downloadFile(dbx, f, dst)
	default:
		return fmt.Errorf("unexpected metadata type: %T", f)
	}
}

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get [flags] <source> [<target>]",
	Short: "Download a file or folder",
	RunE:  get,
}

func init() {
	RootCmd.AddCommand(getCmd)
	getCmd.Flags().IntP("workers", "w", 4, "Number of concurrent download workers to use")
}
