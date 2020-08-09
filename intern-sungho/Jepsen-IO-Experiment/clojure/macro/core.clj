(ns tutorial.core
  (:gen-class)) ; namespace

(defmacro Simple [] (println "Hello"))

(defmacro happy
  []
  `(println "I'm happy!"))

(defn -main [& args]
  (println :expanded (macroexpand '(happy)))
  (happy)
)