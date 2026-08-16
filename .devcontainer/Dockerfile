# Dev container base image.
#
# Exists solely to work around an upstream defect in the Microsoft image, which
# would otherwise be usable via a plain `"image"` key in devcontainer.json.
#
# The problem: mcr.microsoft.com/devcontainers/python:1-3.11-bookworm ships a
# preconfigured Yarn apt source (dl.yarnpkg.com) whose signing key has since
# rotated. Any `apt-get update` in the image fails with:
#
#     NO_PUBKEY 62D54FD4003F6525
#     E: The repository 'https://dl.yarnpkg.com/debian stable InRelease' is not signed.
#
# Dev container *features* run `apt-get update` during the build, so this makes
# the docker-in-docker feature abort (exit 100), which fails container creation
# outright and lands the codespace in an Alpine recovery container with no
# Docker daemon at all. Observed 2026-08-16; see docs/DECISIONS.md.
#
# The fix: this project has no use for Yarn, so drop the source rather than
# chase the rotated key. Matching on the URL (not a filename) keeps this working
# whether the image uses the legacy .list format or deb822 .sources.

FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm

RUN set -eux; \
    # Delete any apt source referencing the Yarn repository.
    grep -rl 'dl\.yarnpkg\.com' /etc/apt/sources.list.d/ 2>/dev/null | xargs -r rm -f; \
    # ...and the same entry if it was written into the main sources list.
    if [ -f /etc/apt/sources.list ]; then \
        sed -i '/dl\.yarnpkg\.com/d' /etc/apt/sources.list; \
    fi; \
    # Prove apt is healthy now. `set -e` means a still-broken repo fails the
    # image build here, with a clear error, instead of failing later inside an
    # opaque feature-install step.
    apt-get update; \
    rm -rf /var/lib/apt/lists/*
