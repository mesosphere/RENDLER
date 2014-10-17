Haskell RENDLER Framework:
========

Once you are ssh'd into the `mesos-demo` VM:

1. Install [Haskell Platform](https://www.haskell.org/platform/)
   Version 2014.2.0.0 for generic 64-bit Linux.
2. `cabal update` to fetch the latest packages.
3. cd into `hostfiles/haskell`
4. `cabal install --only-dependencies`
  - If you have trouble installing the `hs-mesos` package, please fetch
    it from [https://github.com/iand675/hs-mesos](https://github.com/iand675/hs-mesos) and install
    it manually with `cabal install --g`.
5. Build the project with `cabal build`
6. Run the project with `cabal run http://mesosphere.com 127.0.1.1:5050`
