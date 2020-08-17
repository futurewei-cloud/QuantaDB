(ns tutorial.core
  (:gen-class)) ; namespace

(defn -main [& args]

  (println "Create a function with ref")
  (def names (ref []))

  (println "\nCreate a function that appends a string")
  (defn change [newname]
    (dosync
     (alter names conj newname)))

  (change "John")
  (change "Mark")
  (println @names)

  (println "\nCreate two functions with ref")
  (def var1 (ref 10))
  (def var2 (ref 20))
  (println @var1 @var2)

  (println "\nCreate a function that updates the numbers")
  (defn change-value [var1 var2 newvalue]
    (dosync
     (alter var1 - newvalue)
     (alter var2 + newvalue)))

  (change-value var1 var2 20)
  (println @var1 @var2)

)