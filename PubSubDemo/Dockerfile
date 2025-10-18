FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["PubSubDemo/PubSubDemo.csproj", "PubSubDemo/"]
COPY ["Publisher/Publisher.csproj", "Publisher/"]
RUN dotnet restore "PubSubDemo/PubSubDemo.csproj"
COPY . .
WORKDIR "/src/PubSubDemo"
RUN dotnet build "PubSubDemo.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "PubSubDemo.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "PubSubDemo.dll"]

