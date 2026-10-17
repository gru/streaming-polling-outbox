# CdcPolling: multi-stage build .NET 8 (requirements.md 7 — строго net8.0).
# Контекст сборки — ./src: сервис ссылается на библиотеку CdcCommon.
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Слой restore кэшируется отдельно от исходников
COPY CdcPolling/CdcPolling.csproj CdcPolling/
COPY CdcCommon/CdcCommon.csproj CdcCommon/
RUN dotnet restore CdcPolling/CdcPolling.csproj

COPY CdcPolling/ CdcPolling/
COPY CdcCommon/ CdcCommon/
RUN dotnet publish CdcPolling/CdcPolling.csproj -c Release -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0
# curl нужен только для docker healthcheck (/health)
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
USER app
ENTRYPOINT ["dotnet", "CdcPolling.dll"]
