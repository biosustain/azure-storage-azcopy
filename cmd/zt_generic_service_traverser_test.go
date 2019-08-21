package cmd

import (
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	chk "gopkg.in/check.v1"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
)

func (s *genericTraverserSuite) TestBlobFSServiceTraverserWithManyObjects(c *chk.C) {
	bfssu := GetBFSSU()
	bsu := getBSU() // Only used to clean up

	// BlobFS is tested on the same account, therefore this is safe to clean up this way
	cleanBlobAccount(c, bsu)

	containerList := []string{
		generateName("suchcontainermanystorage", 63),
		generateName("containertwoelectricboogaloo", 63),
		generateName("funnymemereference", 63),
		generateName("gettingmeta", 63),
	}

	// convert containerList into a map for easy validation
	cnames := map[string]bool{}
	for _, v := range containerList {
		cnames[v] = true
	}

	objectList := []string{
		generateName("basedir", 63),
		"allyourbase/" + generateName("arebelongtous", 63),
		"sub1/sub2/" + generateName("", 63),
		generateName("someobject", 63),
	}

	objectData := "Hello world!"

	// Generate remote scenarios
	scenarioHelper{}.generateFilesystemsAndFilesFromLists(c, bfssu, containerList, objectList, objectData)

	// deferred container cleanup
	defer func() {
		for _, v := range containerList {
			// create container URLs
			blobContainer := bsu.NewContainerURL(v)
			_, _ = blobContainer.Delete(ctx, azblob.ContainerAccessConditions{})
		}
	}()

	// Generate local files to ensure behavior conforms to other traversers
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, objectList)

	// Create a local traversal
	localTraverser := newLocalTraverser(dstDirName, true, true, func() {})

	// Invoke the traversal with an indexer so the results are indexed for easy validation
	localIndexer := newObjectIndexer()
	err := localTraverser.traverse(localIndexer.store, nil)
	c.Assert(err, chk.IsNil)

	// construct a blob account traverser
	blobFSPipeline := azbfs.NewPipeline(azbfs.NewAnonymousCredential(), azbfs.PipelineOptions{})
	rawBSU := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c).URL()
	blobAccountTraverser := newBlobFSAccountTraverser(&rawBSU, blobFSPipeline, ctx, func() {})

	// invoke the blob account traversal with a dummy processor
	blobDummyProcessor := dummyProcessor{}
	err = blobAccountTraverser.traverse(blobDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*len(containerList))

	for _, storedObject := range blobDummyProcessor.record {
		correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]
		_, cnamePresent := cnames[storedObject.containerName]

		c.Assert(present, chk.Equals, true)
		c.Assert(cnamePresent, chk.Equals, true)
		c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
	}
}

func (s *genericTraverserSuite) TestServiceTraverserWithManyObjects(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()
	testS3 := false // Only test S3 if credentials are present.
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err == nil {
		testS3 = true
	} else {
		c.Log("WARNING: Service level traverser is NOT testing S3")
	}

	// Clean the accounts to ensure that only the containers we create exist
	if testS3 {
		cleanS3Account(c, s3Client)
	}
	// BlobFS is tested on the same account, therefore this is safe to clean up this way
	cleanBlobAccount(c, bsu)
	cleanFileAccount(c, fsu)

	containerList := []string{
		generateName("suchcontainermanystorage", 63),
		generateName("containertwoelectricboogaloo", 63),
		generateName("funnymemereference", 63),
		generateName("gettingmeta", 63),
	}

	// convert containerList into a map for easy validation
	cnames := map[string]bool{}
	for _, v := range containerList {
		cnames[v] = true
	}

	objectList := []string{
		generateName("basedir", 63),
		"allyourbase/" + generateName("arebelongtous", 63),
		"sub1/sub2/" + generateName("", 63),
		generateName("someobject", 63),
	}

	objectData := "Hello world!"

	// Generate remote scenarios
	scenarioHelper{}.generateBlobContainersAndBlobsFromLists(c, bsu, containerList, objectList, objectData)
	scenarioHelper{}.generateFileSharesAndFilesFromLists(c, fsu, containerList, objectList, objectData)
	if testS3 {
		scenarioHelper{}.generateS3BucketsAndObjectsFromLists(c, s3Client, containerList, objectList, objectData)
	}

	// deferred container cleanup
	defer func() {
		for _, v := range containerList {
			// create container URLs
			blobContainer := bsu.NewContainerURL(v)
			fileShare := fsu.NewShareURL(v)

			// Ignore errors from cleanup.
			if testS3 {
				_ = s3Client.RemoveBucket(v)
			}
			_, _ = blobContainer.Delete(ctx, azblob.ContainerAccessConditions{})
			_, _ = fileShare.Delete(ctx, azfile.DeleteSnapshotsOptionNone)
		}
	}()

	// Generate local files to ensure behavior conforms to other traversers
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, objectList)

	// Create a local traversal
	localTraverser := newLocalTraverser(dstDirName, true, true, func() {})

	// Invoke the traversal with an indexer so the results are indexed for easy validation
	localIndexer := newObjectIndexer()
	err = localTraverser.traverse(localIndexer.store, nil)
	c.Assert(err, chk.IsNil)

	// construct a blob account traverser
	blobPipeline := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	rawBSU := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	blobAccountTraverser := newBlobAccountTraverser(&rawBSU, blobPipeline, ctx, func() {})

	// invoke the blob account traversal with a dummy processor
	blobDummyProcessor := dummyProcessor{}
	err = blobAccountTraverser.traverse(blobDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	// construct a file account traverser
	filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	rawFSU := scenarioHelper{}.getRawFileServiceURLWithSAS(c)
	fileAccountTraverser := newFileAccountTraverser(&rawFSU, filePipeline, ctx, func() {})

	// invoke the file account traversal with a dummy processor
	fileDummyProcessor := dummyProcessor{}
	err = fileAccountTraverser.traverse(fileDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	var s3DummyProcessor dummyProcessor
	if testS3 {
		// construct a s3 service traverser
		accountURL := scenarioHelper{}.getRawS3AccountURL(c, "")
		s3ServiceTraverser, err := newS3ServiceTraverser(&accountURL, ctx, func() {})
		c.Assert(err, chk.IsNil)

		// invoke the s3 service traversal with a dummy processor
		s3DummyProcessor = dummyProcessor{}
		err = s3ServiceTraverser.traverse(s3DummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
	}

	records := append(blobDummyProcessor.record, fileDummyProcessor.record...)

	c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*len(containerList))
	c.Assert(len(fileDummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*len(containerList))
	if testS3 {
		c.Assert(len(s3DummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*len(containerList))
		records = append(records, s3DummyProcessor.record...)
	}

	for _, storedObject := range records {
		correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]
		_, cnamePresent := cnames[storedObject.containerName]

		c.Assert(present, chk.Equals, true)
		c.Assert(cnamePresent, chk.Equals, true)
		c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
	}
}

func (s *genericTraverserSuite) TestServiceTraverserWithWildcards(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()
	testS3 := false // Only test S3 if credentials are present.
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err == nil {
		testS3 = true
	} else {
		c.Log("WARNING: Service level traverser is NOT testing S3")
	}

	// Clean the accounts to ensure that only the containers we create exist
	if testS3 {
		cleanS3Account(c, s3Client)
	}
	cleanBlobAccount(c, bsu)
	cleanFileAccount(c, fsu)

	containerList := []string{
		generateName("objectmatchone", 63),
		generateName("objectnomatchone", 63),
		generateName("objectnomatchtwo", 63),
		generateName("objectmatchtwo", 63),
	}

	// load only matching container names in
	cnames := map[string]bool{
		containerList[0]: true,
		containerList[3]: true,
	}

	objectList := []string{
		generateName("basedir", 63),
		"allyourbase/" + generateName("arebelongtous", 63),
		"sub1/sub2/" + generateName("", 63),
		generateName("someobject", 63),
	}

	objectData := "Hello world!"

	// Generate remote scenarios
	scenarioHelper{}.generateBlobContainersAndBlobsFromLists(c, bsu, containerList, objectList, objectData)
	scenarioHelper{}.generateFileSharesAndFilesFromLists(c, fsu, containerList, objectList, objectData)
	if testS3 {
		scenarioHelper{}.generateS3BucketsAndObjectsFromLists(c, s3Client, containerList, objectList, objectData)
	}

	// deferred container cleanup
	defer func() {
		for _, v := range containerList {
			// create container URLs
			blobContainer := bsu.NewContainerURL(v)
			fileShare := fsu.NewShareURL(v)

			// Ignore errors from cleanup.
			if testS3 {
				_ = s3Client.RemoveBucket(v)
			}
			_, _ = blobContainer.Delete(ctx, azblob.ContainerAccessConditions{})
			_, _ = fileShare.Delete(ctx, azfile.DeleteSnapshotsOptionNone)
		}
	}()

	// Generate local files to ensure behavior conforms to other traversers
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, objectList)

	// Create a local traversal
	localTraverser := newLocalTraverser(dstDirName, true, true, func() {})

	// Invoke the traversal with an indexer so the results are indexed for easy validation
	localIndexer := newObjectIndexer()
	err = localTraverser.traverse(localIndexer.store, nil)
	c.Assert(err, chk.IsNil)

	// construct a blob account traverser
	blobPipeline := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	rawBSU := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	rawBSU.Path = "/objectmatch*" // set the container name to contain a wildcard
	blobAccountTraverser := newBlobAccountTraverser(&rawBSU, blobPipeline, ctx, func() {})

	// invoke the blob account traversal with a dummy processor
	blobDummyProcessor := dummyProcessor{}
	err = blobAccountTraverser.traverse(blobDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	// construct a file account traverser
	filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	rawFSU := scenarioHelper{}.getRawFileServiceURLWithSAS(c)
	rawFSU.Path = "/objectmatch*" // set the container name to contain a wildcard
	fileAccountTraverser := newFileAccountTraverser(&rawFSU, filePipeline, ctx, func() {})

	// invoke the file account traversal with a dummy processor
	fileDummyProcessor := dummyProcessor{}
	err = fileAccountTraverser.traverse(fileDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	var s3DummyProcessor dummyProcessor
	if testS3 {
		// construct a s3 service traverser
		accountURL, err := common.NewS3URLParts(scenarioHelper{}.getRawS3AccountURL(c, ""))
		c.Assert(err, chk.IsNil)
		accountURL.BucketName = "objectmatch*" // set the container name to contain a wildcard

		urlOut := accountURL.URL()
		s3ServiceTraverser, err := newS3ServiceTraverser(&urlOut, ctx, func() {})
		c.Assert(err, chk.IsNil)

		// invoke the s3 service traversal with a dummy processor
		s3DummyProcessor = dummyProcessor{}
		err = s3ServiceTraverser.traverse(s3DummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
	}

	records := append(blobDummyProcessor.record, fileDummyProcessor.record...)

	// Only two containers should match.
	c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*2)
	c.Assert(len(fileDummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*2)
	if testS3 {
		c.Assert(len(s3DummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*2)
		records = append(records, s3DummyProcessor.record...)
	}

	for _, storedObject := range records {
		correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]
		_, cnamePresent := cnames[storedObject.containerName]

		c.Assert(present, chk.Equals, true)
		c.Assert(cnamePresent, chk.Equals, true)
		c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
	}
}

func (s *genericTraverserSuite) TestBlobFSServiceTraverserWithWildcards(c *chk.C) {
	bsu := getBSU()
	bfssu := GetBFSSU()

	// BlobFS is tested on the same account, therefore this is safe to clean up this way
	cleanBlobAccount(c, bsu)

	containerList := []string{
		generateName("objectmatchone", 63),
		generateName("objectnomatchone", 63),
		generateName("objectnomatchtwo", 63),
		generateName("objectmatchtwo", 63),
	}

	// load only matching container names in
	cnames := map[string]bool{
		containerList[0]: true,
		containerList[3]: true,
	}

	objectList := []string{
		generateName("basedir", 63),
		"allyourbase/" + generateName("arebelongtous", 63),
		"sub1/sub2/" + generateName("", 63),
		generateName("someobject", 63),
	}

	objectData := "Hello world!"

	// Generate remote scenarios
	scenarioHelper{}.generateFilesystemsAndFilesFromLists(c, bfssu, containerList, objectList, objectData)

	// deferred container cleanup
	defer func() {
		for _, v := range containerList {
			// create container URLs
			blobContainer := bsu.NewContainerURL(v)
			_, _ = blobContainer.Delete(ctx, azblob.ContainerAccessConditions{})
		}
	}()

	// Generate local files to ensure behavior conforms to other traversers
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, objectList)

	// Create a local traversal
	localTraverser := newLocalTraverser(dstDirName, true, true, func() {})

	// Invoke the traversal with an indexer so the results are indexed for easy validation
	localIndexer := newObjectIndexer()
	err := localTraverser.traverse(localIndexer.store, nil)
	c.Assert(err, chk.IsNil)

	// construct a blob account traverser
	blobPipeline := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	rawBSU := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c).URL()
	rawBSU.Path = "/objectmatch*" // set the container name to contain a wildcard
	bfsAccountTraverser := newBlobFSAccountTraverser(&rawBSU, blobPipeline, ctx, func() {})

	// invoke the blob account traversal with a dummy processor
	bfsDummyProcessor := dummyProcessor{}
	err = bfsAccountTraverser.traverse(bfsDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	// Only two containers should match.
	c.Assert(len(bfsDummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*2)

	for _, storedObject := range bfsDummyProcessor.record {
		correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]
		_, cnamePresent := cnames[storedObject.containerName]

		c.Assert(present, chk.Equals, true)
		c.Assert(cnamePresent, chk.Equals, true)
		c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
	}
}
