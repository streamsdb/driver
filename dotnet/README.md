## Docker container

The `Dockerfile` in the root of this project is used to build a release image. This is an image that containes the packed versions of the solution and can be invoked with an NuGet API key to publish the version to NuGet.

```
# build the image
docker build --build-arg VERSION="1.0.1-rc" -t streamsdb/dotnet-driver .
```

```
# release the package
docker run --rm --name push-packages streamsdb/dotnet-driver --api-key $NUGET_TOKEN
```
