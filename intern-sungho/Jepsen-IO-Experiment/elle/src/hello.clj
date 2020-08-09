(ns jepsen.hello
    (:gen-class))

(defn greet [name] (
       println( str "Hello, " name  )
      )
)

