
```
.___            ___.                         _________  
|   | ____  ____\_ |__   ___________  ____   \_   ___ \ 
|   |/ ___\/ __ \| __ \_/ __ \_  __ \/ ___\  /    \  \/ 
|   \  \__\  ___/| \_\ \  ___/|  | \/ /_/  > \     \____
|___|\___  >___  >___  /\___  >__|  \___  /   \______  /
         \/    \/    \/     \/     /_____/           \/ 
```

A C/C++ implementation of [iceberg table spec](https://iceberg.apache.org/spec/)

## Collaboration

Iceberg-C tracks issues in GitHub and prefers to receive contributions as pull requests.

### Building

```
git submodule update --init
mkdir build && cd build
make -j8
```

### Run tests

```
ctest
```
