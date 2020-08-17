(ns tutorial.core
  (:gen-class)) ; namespace


(defn -main [& args]

  ; declare a variable x
  (def x (atom 1))

  ; loop until x is larger than five
  (while ( < @x 5 )
         ( do
           (println @x)
           (swap! x inc)
         )
  )

  ; for-each statement
  (doseq [n [0 1 2]]
    (println n)
  )

  (dotimes [n 5]
    (println n)
  )

  ; loop
  (loop [x 10]
    (when (> x 1)
        (println x)
        (recur (- x 2))
    )
  )

  (for [x [0 1 2 3 4 5]
        :let [y (* x 3)]
        :when (even? y)]
    y)


  (if ( = 2 2)
    (println "Values are equal")
    (println "Values are not equal")
  )

  (if (= 2 2)
    (do
        (println "Both the values are equal")
        (println "true")
    )
    (do
        (println "Both the values are not equal")
        (println "false")
     )
  )


  (if ( and (= 2 2) (= 3 3))
    (println "Values are equal")
    (println "Values are not equal")
  )

  (def x 5)
  (case x 5 (println "x is 5")
          10 (println "x is 10")
          (println "x is neither 5 nor 10"))

  (def x 5)
  (cond
     (= x 5) (println "x is 5")
     (= x 10)(println "x is 10")
     :else (println "x is not defined")
  )

  (def x (even? 0))
  (println x)

  (def x (neg? 2))
  (println x)

  (def x (odd? 3))
  (println x)

  (def x (pos? 3))
  (println x)


)


