storage = {
  first_visit: false,
  fire_relocate: true,
  week_day_deref: {
    0: 'Sonntag',
    1: 'Montag',
    2: 'Dienstag',
    3: 'Mittwoch',
    4: 'Donnerstag',
    5: 'Freitag',
    6: 'Samstag'
  }
}
$(document).ready(function() {
  localStorage.removeItem('first_visit_done');
  if ($('.home').length) {
    if (typeof(Storage) == "undefined") {
      $('#startquestion').html('<div id="browser-error"><h2>Nicht unterstützter Browser</h2><p>Bitte aktualisiere Deinen Browser.<br>Google Chrome 4+, Internet Explorer 8+, Firefox 3.5+, Safari 4+, Opera 11.5+</p><p><a href=""></a></div>').css({bottom: 0});
      $('#startquestion').fadeIn();
      $('#important-links').fadeIn();
    }
    else {
      storage['first_visit'] = null == localStorage.getItem('first_visit_done');
      
      if (storage['first_visit']) {
        $('#startquestion').fadeIn(600);
        $('#important-links').fadeIn(600);
        
        $('#startquestion-type').multiselect({
          buttonClass: 'form-control',
          buttonContainer: '<div class="btn-group bootstrap-multiselect" />',
          numberDisplayed: 1,
          nSelectedText: ' Kategorien',
          allSelectedText: 'Alle Kategorien',
          nonSelectedText: 'bitte wählen',
          includeSelectAllOption: true,
          selectAllText: 'alle auswählen'
        });
        
        // Geo-Live-Suche
        $('#startquestion-location').live_search({
          url: '/search-region-live',
          form: '#startquestion-form',
          input: '#startquestion-location',
          live_box: '#startquestion-location-live',
          submit: '#startquestion-submit',
          livesearch_result_only: true,
          process_result_line: function(result) {
            return('<li data-q="' + result['lat'] + ',' + result['lon'] + '" data-q-descr="' + result['postalcode'] + ' ' + result['name'] + '">' + result['postalcode'] + ' ' + result['name'] + '</li>');
          },
          after_submit: function() {}
        });
        
        $('#startquestion-form').submit(function(evt) {
          evt.preventDefault();
          if ($('#startquestion-location').attr('data-q')) {
            $('#search-location').attr('data-q', $('#startquestion-location').attr('data-q'));
            $('#search-location').val($('#startquestion-location').val());
            $('#search-type').val($('#startquestion-type').val());
            $('#search-type').multiselect('refresh');
            $('#search-form select').change(function(evt) {
              search_stations(false);
            });
            search_stations(true);
          }
        });
      }
      else {
        $('#header').css({ display: 'block' });
        if ($(window).width() >= 768)
          $('#search').fadeIn(200);
        else {
          $('#search').css({ display: 'block' });
        }
      }
      
      // init category icons
      storage['traffic_item_type_icons'] = {
        1: L.VectorMarkers.icon({icon: 'road', prefix: 'fa', markerColor: '#DB5454', iconColor: '#FFFFFF'}),
        2: L.VectorMarkers.icon({icon: 'car', prefix: 'fa', markerColor: '#00B2B4', iconColor: '#FFFFFF'}),
        3: L.VectorMarkers.icon({icon: 'bicycle', prefix: 'fa', markerColor: '#00B44C', iconColor: '#FFFFFF'}),
        4: L.VectorMarkers.icon({icon: 'bar-chart', prefix: 'fa', markerColor: '#DB5454', iconColor: '#FFFFFF'}),
        5: L.VectorMarkers.icon({icon: 'road', prefix: 'fa', markerColor: '#DB5454', iconColor: '#FFFFFF'})
      }
      
      $('#search').css({ top: $('#header').height() });
      
      if ($(window).width() >= 768)
        $('#search').css({ width: '280px', 'padding-left': '15px', 'padding-right': '15px' });
      else
        $('#search').css({ width: '0px', 'padding-left': '0px', 'padding-right': '0px' });

      $('#search-toggle').click(function() {
        if ($('#search').width()) {
          $('#search').animate({ width: '0px', 'padding-left': '0px', 'padding-right': '0px' });
        }
        else {
          $('#search').animate({ width: '280px', 'padding-left': '15px', 'padding-right': '15px' });
        }
      });
      
      $('#search-type').multiselect({
        buttonClass: 'form-control',
        buttonContainer: '<div class="btn-group bootstrap-multiselect" />',
        numberDisplayed: 1,
        nSelectedText: ' Kategorien',
        allSelectedText: 'Alle Kategorien',
        nonSelectedText: 'bitte wählen',
        includeSelectAllOption: true,
        selectAllText: 'alle auswählen'
      });
      
      // occupancy rate slider
      current_date = new Date();
      occupancy_rate_slider = $('#search-occupancy-rate-input').slider({
        min: 0,
        max: 100,
        value: 100,
        tooltip_position: 'bottom',
        tooltip: 'hide',
        id: 'search-occupancy-rate-slider'
      });
      
      $('#search-occupancy-rate-slider').css({width: $('#search-occupancy-rate-input').width() + 24});
      occupancy_rate_slider.slider('relayout');
      
      $('#search-occupancy-rate').val('100 %');
      
      $('#search-occupancy-rate-slider').on('slide', function() {
        $('#search-occupancy-rate').val($('#search-occupancy-rate-slider .min-slider-handle').attr('aria-valuenow') + ' %');
      });
      $('#search-occupancy-rate-slider').on('slideStop', function() {
        $('#search-occupancy-rate').val($('#search-occupancy-rate-slider .min-slider-handle').attr('aria-valuenow') + ' %');
        search_stations(false);
      });
      
      // date slider
      current_date = new Date();
      date_slider = $('#search-date-input').slider({
        min: (current_date.getTime() / 60 / 60 / 24 / 1000) - 1,
        max: (current_date.getTime() / 60 / 60 / 24 / 1000) + 365,
        value: (current_date.getTime() / 60 / 60 / 24 / 1000) - 1,
        tooltip_position: 'bottom',
        tooltip: 'hide',
        id: 'search-date-slider'
      });
      
      $('#search-date-slider').css({width: $('#search-date-input').width() + 24});
      date_slider.slider('relayout');
      
      $('#search-date').val(storage.week_day_deref[current_date.getDay()] + ', der ' + current_date.toLocaleDateString());
      
      $('#search-date-slider').on('slide', function() {
        var current_date = new Date(parseInt($('#search-date-slider .min-slider-handle').attr('aria-valuenow')) * 60 * 60 * 24 * 1000);
        $('#search-date').val(storage.week_day_deref[current_date.getDay()] + ', der ' + current_date.toLocaleDateString());
      });
      $('#search-date-slider').on('slideStop', function() {
        current_date = new Date(parseInt($('#search-date-slider .min-slider-handle').attr('aria-valuenow')) * 60 * 60 * 24 * 1000);
        $('#search-date').val(storage.week_day_deref[current_date.getDay()] + ', der ' + current_date.toLocaleDateString());
        search_stations(false);
      });
      
      $('#search-processed').change(function() {
        search_stations(false);
      });
      
      // Geo-Live-Suche
      $('#search-location').live_search({
        url: '/search-region-live',
        form: '#search-form',
        input: '#search-location',
        live_box: '#search-location-live',
        submit: '#basic-submit',
        livesearch_result_only: true,
        process_result_line: function(result) {
          return('<li data-q="' + result['lat'] + ',' + result['lon'] + '" data-q-descr="' + result['postalcode'] + ' ' + result['name'] + '">' + result['postalcode'] + ' ' + result['name'] + '</li>');
        },
        after_submit: function() {}
      });
      
      $('#search-form select').change(function(evt) {
        search_stations(false);
      });
      
      $('#search-form').submit(function(evt) {
        evt.preventDefault();
        if ($('#search-location').attr('data-q'))
          search_stations(true);
      });
      
      $(window).on('resize', function() {
        if ($(window).width() >= 768) {
          $('#search').css({ width: '280px', 'padding-left': '15px', 'padding-right': '15px' });
        }
        else {
          $('#search').css({ width: '0px', 'padding-left': '0px', 'padding-right': '0px' });
        }
      });
      
      // Init map
      L.Icon.Default.imagePath = '/static/images/leaflet/' ;
      map = new L.Map('map', { zoomControl: false, attributionControl: false });
      var backgroundLayer = new L.TileLayer('https://api.mapbox.com/styles/v1/mapbox/streets-v9/tiles/256/{z}/{x}/{y}?access_token=' + mobilitaet_finden_conf['mapbox_token'], {
        maxZoom: 18, 
        minZoom: 1
      });
      storage['map'] = map;
      L.control
        .zoom({ position: 'bottomleft'})
        .addTo(map);
      map.setView(new L.LatLng(51.163375, 10.447683), 7).addLayer(backgroundLayer);
      
      storage['markers'] =  new L.LayerGroup();
      storage['markers'].addTo(map);
      storage['geojson'] = new L.LayerGroup();
      storage['geojson'].addTo(map);
      
      map.on('moveend', function() {
        if (storage['fire_relocate'] && !storage['first_visit'])
          search_stations(false);
      });
    }
  }
  $(document).keyup(function(e) {
  if (e.keyCode === 27)
    if ($('#traffic-item-details').css('display') == 'block') {
      $('#traffic-item-details').fadeOut();
    }
  });
});


function search_stations(relocate) {
  if (relocate)
    storage['fire_relocate'] = false;
  
  // activate select onchange if first load
  if (storage['first_visit']) {
    localStorage.setItem('first_visit_done', true);
  }
  fq = [];
  var map_border = storage['map'].getBounds();
  if (relocate) {
    lat = parseFloat($('#search-location').attr('data-q').split(',')[0]);
    lon = parseFloat($('#search-location').attr('data-q').split(',')[1]);
    
    // 1px lon = 0,00017578125 degree
    // 1px lat = 0,000087890625 degree
    limits = Array(
      'location.lat>=' + String(lat - 1.2 * (0.000087890625 * $(window).height())),
      'location.lat<=' + String(lat + 1.2 * (0.000087890625 * $(window).height())),
      'location.lon>=' + String(lon - 1.2 * (0.00017578125 * $(window).width())),
      'location.lon<=' + String(lon + 1.2 * (0.00017578125 * $(window).width()))
    );
    limits = Array(
      String(lat - 1.2 * (0.000087890625 * $(window).height())), // lat min
      String(lat + 1.2 * (0.000087890625 * $(window).height())), // lat max
      String(lon - 1.2 * (0.00017578125 * $(window).width())), // lon min
      String(lon + 1.2 * (0.00017578125 * $(window).width())) // lon max
    );
  }
  else {
    lat = storage['map'].getCenter()['lat'];
    lon = storage['map'].getCenter()['lng'];
    limits = Array(
      String(lat - 1.2 * (map_border.getNorth() - map_border.getSouth())),
      String(lat + 1.2 * (map_border.getNorth() - map_border.getSouth())),
      String(lon - 1.2 * (map_border.getEast() - map_border.getWest())),
      String(lon + 1.2 * (map_border.getEast() - map_border.getWest()))
    );
  }
  search_params = {
    pp: 10000,
    l: limits.join(';'),
    traffic_item_type: $('#search-type').val().join(','),
    date: parseInt($('#search-date-input').val()) * 60 * 60 * 24,
    occupancy_rate: $('#search-occupancy-rate-input').val(),
    zoom: ((relocate) ? 13 : storage['map'].getZoom()),
    processed: ($('#search-processed').is(':checked')) ? 1 : 0
  }
  
  $.post('/search/traffic-items', search_params, function(raw_data) {
    $('#loading-overlay').css({'display': 'none'});
    storage['geojson'].clearLayers();
    storage['markers'].clearLayers();
    
    storage['geojson'].addLayer(L.geoJSON(raw_data.response, {
      style: function(feature) {
        return {
          'color': '#FF3333',
          'opacity': 0.6
        }
      },
      onEachFeature: function(feature, layer) {
        if (feature.geometry.type == 'LineString') {
          L.marker(
            [feature.geometry.coordinates[0][1], feature.geometry.coordinates[0][0]],
            { icon: storage.traffic_item_type_icons[feature.properties.traffic_item_type], properties: feature.properties }
          ).on('click', function(current_marker) {
            console.log(current_marker)
            display_details(current_marker.target.options.properties);
          }).addTo(storage['markers']);
        }
      },
      pointToLayer: function(feature, latlng) {
        marker = L.marker(latlng, { icon: storage.traffic_item_type_icons[feature.properties.traffic_item_type] } );
        marker.on('click', function(current_marker) {
          display_details(current_marker.target.feature.properties);
        })
        return(marker);
      }
    }));
  });
  
  if (relocate) {
    if ($('#search-location').attr('data-q')) {
      storage['map'].setView([$('#search-location').attr('data-q').split(',')[0], $('#search-location').attr('data-q').split(',')[1]], 13);
    }
  }
  
  if (storage['first_visit']) {
    storage['map'].options.minZoom = 9;
    $('#important-links').fadeOut(400);
    $('#startquestion').fadeOut(400, function() {
      $('#startquestion').remove();
    });
    $('#search-date-slider').css({ width: '230px' });
    $('#search-occupancy-rate-slider').css({ width: '230px' });
    $('#header').fadeIn();
    if ($(window).width() >= 768) {
      $('#search').fadeIn(200);
    }
    else
      $('#search').css({ display: 'block' });
    
    storage['first_visit'] = false;
  }
  
  if (relocate)
    storage['fire_relocate'] = true;
}

function display_details(properties) {
  var html = '<i class="fa fa-times-circle-o" aria-hidden="true" id="traffic-item-details-close"></i><h2>Details</h2><ul>';
  $.each(properties, function(key, value) {
    html += '<li>' + key + ': ' + value + '</li>';
  });
  html += '</ul>';
  $('#traffic-item-details').html(html).css({ top: $('#header').outerHeight() + 'px', right: $('#search').outerWidth() + 'px', bottom: $('#map-attribution').outerHeight() + 'px' }).fadeIn();
    $('#traffic-item-details-close').click(function() {
      $('#traffic-item-details').fadeOut();
    });
}

