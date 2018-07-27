function initMap(){
    var options = {
        center: {lat: 39.8283, lng: -98.5795},
        zoom: 4.5
    };
    var map = new google.maps.Map(document.getElementById("map"), options);
    /*
    var marker = new google.maps.Marker({
        position: {lat: 47.6194452, lng: -122.3481112},
        map: map
    });
    var infoWindow = new google.maps.InfoWindow({
        content: "<h3>Sinclair Digital</h3>"
    });
    marker.addListener("click", function(){
        infoWindow.open(map, marker);
    });
    */
    var markers = [
        {
          coords: {lat: 47.6062, lng: -122.3321},
          content: "<h3>Sinclair Digital</h3>"
        },
        {
          coords: {lat: 45.6062, lng: -120.3321},
          content: "<h3>foo bar</h3>"
        }
    ];
    // var markers = JSON.parse(coords)
    for(var i = 0; i < markers.length; i ++){
        addMarker(markers[i]);
    };
    function addMarker(props){
        var marker = new google.maps.Marker({
            position: props.coords,
            map: map
            });
        // var marker = new google.maps.Marker({
        //     position: props,
        //     map: map
        //     });
        if (props.content){
            var infoWindow = new google.maps.InfoWindow({
                content: props.content
            });
            marker.addListener("click", function(){
                infoWindow.open(map, marker);
            });

        };
    };
};

