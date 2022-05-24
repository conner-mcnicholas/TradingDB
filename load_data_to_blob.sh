azcopy login
azcopy make 'https://asaworkspacewin.blob.core.windows.net/mycontainer'
unzip data.zip
azcopy copy '/home/conner/Downloads/data' 'https://asaworkspacewin.blob.core.windows.net/mycontainer' --recursive
