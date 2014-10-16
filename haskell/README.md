Haskell RENDLER Framework:
========

Once you are ssh'd into the `mesos-demo` VM:

1. Install GHC >= 7.8 and Cabal >= 1.10
2. cd into `hostfiles/haskell`
3. `cabal install --only-dependencies`
  - If you have trouble installing the `hs-mesos` package, please fetch
    it from [https://github.com/iand675/hs-mesos](https://github.com/iand675/hs-mesos) and install
    it manually with `cabal install --g`.
4. Run the project with `cabal run http://mesosphere.com 127.0.1.1:5050`
