package main

import(
    "fmt"
    "encoding/json"
)

type FruitBasket struct {
    Name    string
    Fruit   []string
    Id      int64
}

func main() {
    
    example_basket := FruitBasket{
        Name: "Bhavana",
        Fruit: []string{"banana", "apple"},
        Id: 546,
    }

    bytes_basket, _ := json.Marshal(example_basket)
    var basket FruitBasket
    err := json.Unmarshal(bytes_basket, &basket)
    if err != nil {
        fmt.Println(err)
    }
    fmt.Println(basket.Name, basket.Fruit, basket.Id)
}

