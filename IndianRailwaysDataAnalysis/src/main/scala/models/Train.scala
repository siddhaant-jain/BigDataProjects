package models
    
case class Geometry(val coordinates: Array[Array[Double]], typeOfGeometry: String)

case class Train(val geometry: Geometry, properties: TrainProperty, val typeCol: String)