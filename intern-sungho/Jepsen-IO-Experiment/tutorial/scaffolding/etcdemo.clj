; namespace and the included external APIs
(ns jepsen.etcdemo
    ; jepsen.cli creates the workers
    (:require [jepsen.cli :as cli]
    ; jepsen.tests contains a noop-test that runs by the workers
              [jepsen.tests :as tests]))

; the function that runs the etcd test
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  ; function is extended with the noop-test
  ; noop-test does nothing
  (merge tests/noop-test
         opts))

; main function
(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn etcd-test})
            args))