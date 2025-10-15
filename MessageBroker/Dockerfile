FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
WORKDIR /app
EXPOSE 9096

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["MessageBroker/MessageBroker.csproj", "MessageBroker/"]
RUN dotnet restore "MessageBroker/MessageBroker.csproj"
COPY . .
WORKDIR "/src/MessageBroker"
RUN dotnet build "MessageBroker.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "MessageBroker.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MessageBroker.dll"]