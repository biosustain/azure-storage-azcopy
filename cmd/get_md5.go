// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/spf13/cobra"
)

func init() {
	var sourcePath = ""

	getMD5Cmd := &cobra.Command{
		Use:     "get_md5 [blobURL]",
		Aliases: []string{},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("this command only requires container destination")
			}
			sourcePath = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			// the expected argument in input is the container sas / or path of virtual directory in the container.
			// verifying the location type
			location := inferArgumentLocation(sourcePath)
			if location != location.Blob() {
				glcm.Error("invalid path passed for listing. given source is of type " + location.String() + " while expect is container / container path ")
			}

			err := HandleGetMD5Cmd(sourcePath)
			if err == nil {
				glcm.Exit(nil, common.EExitCode.Success())
			} else {
				glcm.Error(err.Error())
			}

		},
	}

	rootCmd.AddCommand(getMD5Cmd)
}

// HandleGetMD5Cmd handles the list container command
func HandleGetMD5Cmd(source string) (err error) {
	// TODO: Temporarily use context.TODO(), this should be replaced with a root context from main.
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	credentialInfo := common.CredentialInfo{}
	// Use source as resource URL, and it can be public access resource URL.
	if credentialInfo.CredentialType, _, err = getBlobCredentialType(ctx, source, true, false); err != nil {
		return err
	} else if credentialInfo.CredentialType == common.ECredentialType.OAuthToken() {
		// Message user that they are using Oauth token for authentication,
		// in case of silently using cached token without consciousness。
		glcm.Info("List is using OAuth token for authentication.")

		uotm := GetUserOAuthTokenManagerInstance()
		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return err
		} else {
			credentialInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	// attempt to parse the source url
	sourceURL, err := url.Parse(source)
	if err != nil {
		return errors.New("cannot parse source URL")
	}

	// Create Pipeline which will be used further in the blob operations.
	p, err := createBlobPipeline(ctx, credentialInfo)
	if err != nil {
		return err
	}

	blobURL := azblob.NewPageBlobURL(*sourceURL, p)
	blobProps, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		return err
	}

	util := copyHandlerUtil{}

	fmt.Printf("MD5: %x\t%d\t%s\n",
		blobProps.ContentMD5(),
		blobProps.ContentLength(),
		util.getBlobNameFromURL(sourceURL.Path))

	return nil
}
