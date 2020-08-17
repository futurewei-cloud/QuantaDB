(ns jepsen.redis.core
    "Top-level test runner, integration point for various workloads and nemeses."
    (:require [elle.list-append :as a]
              [clojure [pprint :refer [pprint]]
               [string :as str]]))

(defn -main
  [& args]

    (if (empty? args)
        ( (println "No arguments")
          (System/exit 0) )
    )

    ; G0: A write cycle == (P0) Dirty Write
    (def G0 [
              ; transaction 1
              {:type :ok, :value [[:append :x 1] [:append :y 1]]} ; x[1] y[1]
              ; transaction 2
              {:type :ok, :value [[:append :x 2] [:append :y 2]]}
              ; transaction 3: reads updated x and y from both T1 and T2
              {:type :ok, :value [[:r :x [1 2]] [:r :y [2 1]]]}
              ; transaction 3 should have x[1,2] y[1,2] or x[2,1] y[2,1]
            ])

    ; G1a: Aborted Read, T2 sees T1's failed write
    (def G1a [
              ; transaction 1
              {:type :fail, :value [[:append :x 1]]}
              ; transaction 2: reads the aborted x from T1
              {:type :ok, :value [[:r :x [1]]]}
              ; transaction 2 should be aborted
             ])

    ; G1b: Intermediate Reads, T2 sees T1's intermediate write == Fuzzy Read
    (def G1b [
              ; transaction 1
              {:type :ok, :value [[:append :x 1] [:append :x 2]]}
              ; transaction 2
              {:type :ok, :value [[:r :x [1]]]}
              ; transaction 2 should have x[1,2]
             ])

    ; G1c: Circular Information Flow, T2 writes x after T1, but T1 observes T2's write on y.
    (def G1c [
              ; transaction 1
              {:type :ok, :value [[:append :x 1] [:r :y [1]]]}
              ; transaction 2
              {:type :ok, :value [[:append :x 2] [:append :y 1]]}
              ; transaction 3: T1 -> T2 but T1 seen T2's write
              {:type :ok, :value [[:r :x [1 2]] [:r :y [1]]]}
              ; either transaction 1 or 2 have to be aborted
              ])

    ; G2: Anti-dependency Cycles
    (def G2 [
               ; transaction 1: read y before y was updated by T2
               {:type :ok, :value [[:append :x 1] [:r :y]]}
               ; transaction 2: read x before x was updated by T1
               {:type :ok, :value [[:append :y 1] [:r :x]]}
               ])

    ; incompatible-order == Lost Update
    (def LostUpdate [
               ; transaction 1
               {:type :ok, :value [[:append :x 1] [:r :x [1]]]}
               ; transaction 2
               {:type :ok, :value [[:append :x 2] [:r :x [1 2]]]}
               ; transaction 3: T1 update is lost
               {:type :ok, :value [[:r :x [2]]]}
               ; transaction 3 should have [1,2]
               ])

    (let [testcase (first args)]
      (case testcase
        "G0" (pprint (a/check {:consistency-models [:serializable], :directory "out"} G0))
        "G1a" (pprint (a/check {:consistency-models [:serializable], :directory "out"} G1a))
        "G1b" (pprint (a/check {:consistency-models [:serializable], :directory "out"} G1b))
        "G1c" (pprint (a/check {:consistency-models [:serializable], :directory "out"} G1c))
        "G2" (pprint (a/check {:consistency-models [:serializable], :directory "out"} G2))
        "LostUpdate" (pprint (a/check {:consistency-models [:serializable], :directory "out"} LostUpdate))))
  )
