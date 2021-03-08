db.carta.drop()
db.carta.insertMany([
    {
        nombre: "Serranito especial",
        kcal: 780,
        nutricion: {carbo: 250, proteina: 45, grasas: 40},
        ingredientes: [
            {idIngr:1, cantidad:140}, {idIngr:2, cantidad:100}, {idIngr:3, cantidad:25}, {idIngr:4, cantidad:25}
        ],
        productoEstrella: true,
        idCategoria: 1
    },

    {nombre: "Lasaña", kcal: 560, nutricion: {carbo: 123, proteina: 30, grasas: 25}, ingredientes: [
        {idIngr:5, cantidad:150}, {idIngr:3, cantidad:40}, {idIngr:6, cantidad:100}
    ], productoEstrella: true, idCategoria: 2}, 
    
    {nombre: "Pez Espada", kcal: 500, nutricion: {carbo: 60, proteina: 70, grasas: 20}, ingredientes: [
        {idIngr:7, cantidad:300}, {idIngr:8, cantidad:130}
    ], productoEstrella: false, idCategoria: 3},

    {nombre: "Pollo al curry", kcal: 700, nutricion: {carbo: 60, proteina: 80, grasas: 40}, ingredientes: [
        {idIngr:9, cantidad:130}, {idIngr:10, cantidad:15}
    ], productoEstrella: true, idCategoria: 2},

    {nombre: "Pulpo con patatas", kcal: 650, nutricion: {carbo: 80, proteina: 46, grasas: 35}, ingredientes: [
        {idIngr:11, cantidad:120}, {idIngr:8, cantidad:140}
    ], productoEstrella: false, idCategoria: 3},

    {nombre: "Ensalada de la casa", kcal: 350, nutricion: {carbo: 20, proteina: 20, grasas: 15}, ingredientes: [
        {idIngr:3, cantidad:25}, {idIngr:12, cantidad: 50}, {idIngr:13, cantidad:30}, {idIngr:14, cantidad:7}
    ], productoEstrella: false, idCategoria: 4},

    {nombre: "Hamburgesa de la casa", kcal: 700, nutricion: {carbo: 70, proteina: 35, grasas: 35}, ingredientes: [
        {idIngr:15, cantidad:100}, {idIngr:16, cantidad:110}, {idIngr:3, cantidad:20}, {idIngr:12, cantidad: 25}, {idIngr:10, cantidad:8}
    ], productoEstrella: true, idCategoria: 1},

    {nombre: "Burrito especial", kcal: 800, nutricion: {carbo: 60, proteina: 35, grasas: 55}, ingredientes: [
        {idIngr:5, cantidad:100}, {idIngr:10, cantidad:8}, {idIngr:4, cantidad:20}, {idIngr:3, cantidad:10}, {idIngr:17, cantidad:5}
    ], productoEstrella: true, idCategoria: 1},

    {nombre: "Solomillo a la pimienta", kcal: 680, nutricion: {carbo: 60, proteina: 45, grasas: 30}, ingredientes: [
        {idIngr:18, cantidad:150}, {idIngr:19, cantidad: 25}, {idIngr:8, cantidad:120}
    ], productoEstrella: false, idCategoria: 2}
])

db.ingredientes.drop()
db.ingredientes.insertMany([
    {
        idIngr: 1,
        nombreIng: "Pan",
        precioCompra: 2.10,
        fechaCaducidad: new Date("2021-03-26")
    },

    {idIngr: 2, nombreIng: "Filete Lomo", precioCompra: 40, fechaCaducidad: new Date("2021-05-12")},
    {idIngr: 3, nombreIng: "Tomate", precioCompra: 1.30, fechaCaducidad: new Date("2021-03-21")},
    {idIngr: 4, nombreIng: "Pimiento", precioCompra: 1.55, fechaCaducidad: new Date("2021-03-30")},
    {idIngr: 5, nombreIng: "Carne picada", precioCompra: 6.70, fechaCaducidad: new Date("2021-05-27")},
    {idIngr: 6, nombreIng: "Bechamel", precioCompra: 2.10, fechaCaducidad: new Date("2021-04-16")},
    {idIngr: 7, nombreIng: "Pez Espada", precioCompra: 14, fechaCaducidad: new Date("2021-04-01")},
    {idIngr: 8, nombreIng: "Patata", precioCompra: 0.70, fechaCaducidad: new Date("2021-03-15")},
    {idIngr: 9, nombreIng: "Pollo", precioCompra: 5, fechaCaducidad: new Date("2021-05-17")},
    {idIngr: 10, nombreIng: "Curry", precioCompra: 5.10, fechaCaducidad: new Date("2021-04-21")},
    {idIngr: 11, nombreIng: "Pulpo", precioCompra: 35, fechaCaducidad: new Date("2021-06-12")},
    {idIngr: 12, nombreIng: "Lechuga", precioCompra: 0.90, fechaCaducidad: new Date("2021-03-25")},
    {idIngr: 13, nombreIng: "Atún", precioCompra: 20, fechaCaducidad: new Date("2021-09-30")},
    {idIngr: 14, nombreIng: "Vinagre", precioCompra: 1.15, fechaCaducidad: new Date("2022-05-19")},
    {idIngr: 15, nombreIng: "Pan hamburguesa", precioCompra: 4.89, fechaCaducidad: new Date("2021-03-26")},
    {idIngr: 16, nombreIng: "Hamburgesa", precioCompra: 5.75, fechaCaducidad: new Date("2021-06-26")},
    {idIngr: 17, nombreIng: "Guindilla", precioCompra: 40, fechaCaducidad: new Date("2021-04-15")},
    {idIngr: 18, nombreIng: "Solomillo", precioCompra: 29.90, fechaCaducidad: new Date("2021-04-13")},
    {idIngr: 19, nombreIng: "Salsa a la pimienta", precioCompra: 2.30, fechaCaducidad: new Date("2021-03-26")}
])

db.categoria.drop()
db.categoria.insertMany([
    {
        idCategoria: 1,
        nombreCat: "Bocadillos",
        precioAñadido: 1.30
    },

    {idCategoria: 2, nombreCat: "Carnicos", precioAñadido: 1.55},
    {idCategoria: 3, nombreCat: "Pescados", precioAñadido: 1.70},
    {idCategoria: 4, nombreCat: "Vegetales", precioAñadido: 1.30},
])