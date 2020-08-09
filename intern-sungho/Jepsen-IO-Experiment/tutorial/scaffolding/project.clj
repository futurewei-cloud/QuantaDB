; project.clj specifies how to build and run the code
(defproject jepsen.etcdemo "0.1.0-SNAPSHOT"
  :description "A Jepsen test for etcd"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  ; the main function that will be invoked first
  :main jepsen.etcdemo
  ; specifies the 3rd party libraries
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.1.13"]
                 ; a library for talking to etcd
                 [verschlimmbesserung "0.1.3"]])