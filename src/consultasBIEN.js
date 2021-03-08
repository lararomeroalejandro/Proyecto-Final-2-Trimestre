/* 
    1ª consulta: 
    Obtener el coste de elaboración de cada plato ordenados de más barato a más caro. 
*/

db.carta.aggregate([
    {
        $unwind: "$ingredientes"
    },{
        $lookup: {
            from: "ingredientes",
            localField: "ingredientes.idIngr",
            foreignField: "idIngr",
            as: "composicion"
        }
    },{
        $set: {
            precioIngrediente: {$arrayElemAt: ["$composicion.precioCompra", 0]}
        }
    },{
        $project: {
            _id: 0,
            nombrePlato: "$nombre",
            precioIngrediente: 1,
            precioTotalIngrediente: {$round: {$multiply: ["$ingredientes.cantidad", "$precioIngrediente"]}},
        }
    },{
        $project: {
            _id: 0,
            nombrePlato: "$nombrePlato",
            precioFinalIngrediente: {$divide: ["$precioTotalIngrediente", 1000]}
        }
    },{
        $group: {
            _id: "$nombrePlato",
            costeElaboracion: {$sum: "$precioFinalIngrediente"}
        }
    },{
        $sort: {
            costeElaboracion: -1
        }
    }
]).pretty()

/* 
    2ª consulta: 
    Calculo el precio de venta de cada plato, resultado de la aplicación de un impuesto al coste de fabricación de cada plato. Y, además
    obtengo el beneficio de que se obtiene con la venta de cada plato.
*/

db.carta.aggregate([
    {
        $unwind: "$ingredientes"
    },{
        $lookup: {
            from: "ingredientes",
            localField: "ingredientes.idIngr",
            foreignField: "idIngr",
            as: "composicion"
        }
    },{
        $lookup: {
            from: "categoria",
            localField: "idCategoria",
            foreignField: "idCategoria",
            as: "impuestos"
        }
    },{
        $set: {
            precioIngrediente: {$arrayElemAt: ["$composicion.precioCompra", 0]},
            añadido: {$arrayElemAt: ["$impuestos.precioAñadido", 0]}
        }
    },{
        $project: {
            _id: 0,
            nombrePlato: "$nombre",
            precioIngrediente: 1,
            precioTotalIngrediente: {$round: {$multiply: ["$ingredientes.cantidad", "$precioIngrediente"]}},
            añadido: 1,
            nombreIngre: "$composicion.nombreIng"
        }
    },{
        $project: {
            _id: 0,
            nombrePlato: "$nombrePlato",
            precioFinalIngrediente: {$divide: ["$precioTotalIngrediente", 1000]},
            imp: "$añadido"
        }
    },{
        $group: {
            _id: "$nombrePlato",
            costeElaboracion: {$sum: "$precioFinalIngrediente"},
            iva: {$push: "$imp"}
        }
    },{
        $project: {
            costeElaboracion: "$costeElaboracion",
            impuesto: {$arrayElemAt: ["$iva", 0]},
        }
    },{
        $project: {
            costeElaboracion: "$costeElaboracion",
            precioVenta: {$multiply: ["$costeElaboracion", "$impuesto"]}
        }
    },{
        $project: {
            costeElaboracion: "$costeElaboracion",
            precioVenta: "$precioVenta",
            beneficioNeto: {$subtract: ["$precioVenta", "$costeElaboracion"]}
        }
    }
]).pretty()

/* 
    3ª consulta: 
    Muestra los platos, los ingredientes que lo forman y si es un plato recomendable si quieres perder peso en función de sus calorías.
*/

db.carta.aggregate([
    {
        $lookup: {
            from: "ingredientes",
            localField: "ingredientes.idIngr",
            foreignField: "idIngr",
            as: "composicion"
        }
    },{
        $project: {
            _id: 0,
            nombrePlato: "$nombre",
            ingredientes: "$composicion.nombreIng",
            kcal: "$kcal",
            perderPeso: 
                {$cond: {
                    if: {
                        $lte: ["$kcal", 500]
                    }, 
                        then: "Recomendable", else: "No recomendable"
                    }
                }
        }
    },{
        $sort: {
            kcal: -1
        }
    }
]).pretty()

/* 
    4ª consulta: 
    Agrupar según intervalos los platos mediante sus calorias. Se mostrarán cuantos platos hay en cada intervalo, el nombre del mismo y 
    sus calorías exactas.
*/

db.carta.aggregate([
    {
        $lookup: {
            from: "categoria",
            localField: "idCategoria",
            foreignField: "idCategoria",
            as: "categorias"
        }
    },{
        $project: {
            _id: 0,
            nombrePlato: "$nombre",
            calorias: "$kcal"
        }
    },{
        $bucket: {
            groupBy: "$calorias",
            boundaries: [0, 300, 600, 800],
            default: "Muy denso calóricamente",
            output: {
                cuantos: {$sum: 1},
                cuales: {$addToSet: "$nombrePlato"},
                caloriasExactas: {$addToSet: "$calorias"}
            }
        }
    }
]).pretty()


/* 
    5ª consulta: 
    Averigua los ingredientes que caducarán en el próximo mes, y como de urgente es reponerlos en función del número de platos que necesiten
    dicho ingrediente.
*/

db.carta.aggregate([
    {
        $unwind: "$ingredientes"
    },{
        $lookup: {
            from: "ingredientes",
            localField: "ingredientes.idIngr",
            foreignField: "idIngr",
            as: "composicion"
        }
    },{
        $project: {
            _id: 0,
            nombrePlato: "$nombre",
            ingre: "$composicion.nombreIng",
            caducidad: "$composicion.fechaCaducidad",
            precioCompra: "$composicion.precioCompra"
        } 
    },{
        $match: {
            caducidad: {
                $lte: new Date("2021-04-30")
            }
        }
    },{
        $project: {
            ingredientesPorCaducar: "$ingre",
            precioCompra: "$precioCompra"
        }
    },{
        $group: {
            _id: "$ingredientesPorCaducar",
            precio: {$addToSet: "$precioCompra"},
            count: {$sum: 1},
            
        }
    },{
        $project: {
            condisiones: {
                $cond: {
                    if: {
                        $gte: ["$count", 3]
                    },
                        then: "Urge", else: "No urge tanto"
                }
            }
        }
    }
]).pretty()

/*
    Otra forma de realizar esta consulta es con el comando $bucket
*/

db.carta.aggregate([
    {
        $unwind: "$ingredientes"
    },{
        $lookup: {
            from: "ingredientes",
            localField: "ingredientes.idIngr",
            foreignField: "idIngr",
            as: "composicion"
        }
    },{
        $project: {
            _id: 0,
            nombrePlato: "$nombre",
            ingre: "$composicion.nombreIng",
            caducidad: "$composicion.fechaCaducidad",
            precioCompra: "$composicion.precioCompra"
        } 
    },{
        $match: {
            caducidad: {
                $lte: new Date("2021-04-30")
            }
        }
    },{
        $project: {
            ingredientesPorCaducar: "$ingre",
            precioCompra: "$precioCompra"
        }
    },{
        $group: {
            _id: "$ingredientesPorCaducar",
            precio: {$addToSet: "$precioCompra"},
            count: {$sum: 1},
            
        }
    },{
        $bucket: {
            groupBy: "$count",
            boundaries: [0, 2, 3],
            default: "Gran urgencia",
            output: {
                cuantos: {$sum: 1},
                cuales: {$addToSet: "$_id"},
            }
        }
    }
]).pretty()
