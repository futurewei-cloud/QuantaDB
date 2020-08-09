(ns jepsen.etcdemo
    (:require [clojure.tools.logging :refer :all]
              [clojure.string :as str]
              [jepsen [cli :as cli]
               [control :as c]
               [db :as db]
               [tests :as tests]]
              [jepsen.control.util :as cu]
              [jepsen.os.debian :as debian]))

(def dir "/opt/etcd")
(def binary "etcd")
(def logfile (str dir "/etcd.log"))
(def pidfile (str dir "/etcd.pid"))

; a collection of functions used for installing the etcd
(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" (name node) ":" port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node 2380))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (node-url node 2379))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str node "=" (peer-url node))))
       (str/join ",")))

; database function
(defn db
  "Etcd DB for a particular version."
  [version]
  (reify db/DB
         ; starts the workers and install etcd
         (setup! [_ test node]
                 (info node "installing etcd" version)
                 ; uses the sudo command
                 (c/su
                  ; download the tar file from the website
                  (let [url (str "https://storage.googleapis.com/etcd/" version
                                 "/etcd-" version "-linux-amd64.tar.gz")]
                    (cu/install-archive! url dir))

                  ; follows the clustering procedures by invoking each functions
                  (cu/start-daemon!
                   {:logfile logfile
                    :pidfile pidfile
                    :chdir   dir}
                   binary
                   :--log-output                   :stderr
                   :--name                         (name node)
                   :--listen-peer-urls             (peer-url   node)
                   :--listen-client-urls           (client-url node)
                   :--advertise-client-urls        (client-url node)
                   :--initial-cluster-state        :new
                   :--initial-advertise-peer-urls  (peer-url node)
                   :--initial-cluster              (initial-cluster test))
                  ))

         ; stops the workers and removes the etcd
         (teardown! [_ test node]
                    (info node "tearing down etcd")
                    (cu/stop-daemon! binary pidfile)
                    (c/su (c/exec :rm :-rf dir)))

         ; store the log files
         db/LogFiles
         (log-files [_ test node]
                    [logfile])
         ))


(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "etcd"
          :os   debian/os
          ; database function assigned as etcd
          :db   (db "v3.1.5")}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn etcd-test})
            args))