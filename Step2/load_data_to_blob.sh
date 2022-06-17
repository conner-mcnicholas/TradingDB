azcopy login --tenant-id 2f9a9629-3599-4272-ac5e-cd4c5a76d072
azcopy make 'https://pipelinestorageacctaus.blob.core.windows.net/pipelineauscontainer'
azcopy copy '/home/conner/Downloads/data' 'https://pipelinestorageacctaus.blob.core.windows.net/pipelineauscontainer' --recursive
