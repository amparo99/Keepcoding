d3.json('practica_airbnb.json')
    .then((featureCollection) => {
        console.log(featureCollection.features)
        drawMap(featureCollection)
    });


function drawMap(featureCollection) {

    var width = 600;
    var height = 800;
    var margin = 100;

    var svg = d3.select('div') //lienzo para mapa
        .append('svg')
        .attr('width', width + margin)
        .attr('height', height + margin)
        .append('g')
        .attr("transform", "translate(50, 20)");

    var svg2 = d3.select('div') //lienzo para el gráfico de barras
        .append('svg')
        .attr('width', width + margin)
        .attr('height', height + margin)
        .append('g')
        .attr("transform", "translate(120 , 240)");

    var tooltip = d3.select("div") //leyenda para mostrar info en el mapa
        .append("div")
        .attr("class", "tooltip")
        .style("position", "absolute")
        .style("pointer-events", "none")
        .style("visibility", "hidden")
        .style("background-color", "white")
        .style("border", "solid")
        .style("border-width", "1px")
        .style("border-height", "5px")

    //DIBUJAR MAPA

    //1. Datos GeoJSON
    var center = d3.geoCentroid(featureCollection)

    //2.Proyeccion de coordenadas [long,lat] en valores X,Y
    var projectionMercator = d3.geoMercator()
        .fitSize([width, height], featureCollection)


    //3.Crear paths a partir de coordenadas proyectadas.
    var pathGenerator = d3.geoPath().projection(projectionMercator);

    var avgpricerange = d3.extent(featureCollection.features, (d) => d.properties.avgprice);

    var scaleColor = d3.scaleLinear()
        .domain(avgpricerange)
        .range(["#F3E1DE", "red"]);

    function ColorDecision(d, i) {
        price = featureCollection.features[i].properties.avgprice;
        //console.log("price: " + price + " color: " + scaleColor(price));
        (isNaN(price)) ? color = "white" : color = scaleColor(price);
        return color;
    }

    var pathMadrid = svg.append("g")
        .selectAll("path")
        .data(featureCollection.features)
        .enter()
        .append("path")
        .attr("d", pathGenerator) //(d) => pathGenerator(d) pero se puede simplificar pq el metodo ya lo detecta
        .attr("fill", ColorDecision)
        .attr("stroke", "black")
        .attr("stroke-width", 1);

    //interacciones 
    d3.selectAll("path")
        .on("mouseover", handleMouseOver)
        .on("mouseout", handleMouseOut)
        .on("click", handleClick);

    function handleMouseOver(d, i) {
        tooltip
            .style("visibility", "visible")
            .style("left", (d3.event.pageX + 20) + "px")
            .style("top", (d3.event.pageY - 30) + "px")
            .text("name: " + featureCollection.features[i].properties.name + " price: " + featureCollection.features[i].properties.avgprice)
    }

    function handleMouseOut(d, i) {
        tooltip
            .style("visibility", "hidden")
            .style("left", (d3.event.pageX + 20) + "px")
            .style("top", (d3.event.pageY - 30) + "px")
            .text("name: " + featureCollection.features[i].properties.name + " price: " + featureCollection.features[i].properties.avgprice)
    }

    function handleClick(d, i) {
        var total = getTotal(i);
        var ymax = d3.max(total);
        scaley.domain([0, ymax]);
        d3.select("#yaxis")
            .transition()
            .duration(1000)
            .call(Yaxis);
        //barras
        d3.selectAll(".barras")
            .transition()
            .duration(1000)
            .attr("y", function (d, i) {
                return scaley(total[i])
            })
            .attr("height", function (d, i) {
                return height / 2 - scaley(total[i])
            });

        var barrioName = featureCollection.features[i].properties.name;
        d3.selectAll("#title")
            .transition()
            .duration(1000)
            .text("Barrio: " + barrioName);
    }



    //DIBUJAR GRAFICO DE BARRAS PARA BARRIO 0

    var barrio = 0;
    var barrioName = featureCollection.features[barrio].properties.name;

    //eje x --> numero de habitaciones
    var length = featureCollection.features[barrio].properties.avgbedrooms.length; //por si en un futuro o en algun caso se incluyen mas habs
    var number_habs = [];
    for (index = 0; index < length; index = index + 1) { number_habs.push(index) }

    var scalex = d3.scaleBand()
        .domain(number_habs)
        .range([0, width])
        .padding(0.5);

    //eje y --> total casas 
    function getTotal(barrio) { //coge los datos de numero de habs en un barrio
        var total = []
        for (habs in number_habs) {
            total.push(featureCollection.features[barrio].properties.avgbedrooms[habs].total)
        }
        return total
    }
    var total = getTotal(barrio);
    console.log(total);
    var ymax = d3.max(total);

    var scaley = d3.scaleLinear()
        .domain([0, ymax])
        .range([height / 2, 0]);

    //crear los ejes
    var Xaxis = d3.axisBottom(scalex);
    var Yaxis = d3.axisLeft(scaley);

    //barras
    var rect = svg2.append("g")
        .selectAll("rect")
        .data(total)
        .enter()
        .append("rect")
        .attr("x", function (d, i) {
            return (scalex(i))
        })
        .attr("y", function (d, i) {
            return scaley(total[i])
        })
        .attr("width", scalex.bandwidth())
        .attr("height", function (d, i) {
            return height / 2 - scaley(total[i])
        })
        .attr("fill", "#ADD6FB")
        .attr("class", "barras");

    //dibujar ejes
    svg2.append("g").attr("transform", "translate(0," + height / 2 + ")").call(Xaxis);
    svg2.append("g").attr("id", "yaxis").call(Yaxis);

    //añadir nombre de ejes
    svg2.append("text")
        .attr("transform", "translate(" + (width / 2) + " , " + (height / 2 + 40) + ")")
        .style("text-anchor", "middle")
        .text("Nº de habitaciones");

    svg2.append("text")
        .attr("transform", "translate(" + (-40) + ", " + (height / 4) + ")rotate(-90)")
        .style("text-anchor", "middle")
        .text("Total de casas");

    var title = svg2.append("text")
        .attr("x",width/2)
        .attr("y", -30)
        //.attr("transform", "translate(" + (width / 2) + ", " + (-30) + ")")
        .style("text-anchor", "middle")
        .style("font-size", "25px")
        .text("Barrio: " + barrioName)
        .attr("id", "title");

}

