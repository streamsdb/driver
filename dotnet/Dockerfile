FROM mcr.microsoft.com/dotnet/core/sdk:2.2 AS builder

ARG VERSION  
ARG CONFIGURATION="Debug"
WORKDIR /sln

COPY . .

RUN dotnet restore
RUN dotnet build /p:Version=$VERSION -c $CONFIGURATION --no-restore  
RUN dotnet pack ./Driver /p:Version=$VERSION -c $CONFIGURATION -Symbols -SymbolPackageFormat snupkg --no-restore --no-build -o /sln/artifacts 

ENTRYPOINT ["dotnet", "nuget", "push", "/sln/artifacts/*.nupkg"]