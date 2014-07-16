;(function(undefined) {

  // Namespace
  //-----------
  var app = {
    token: undefined,
    sigma: {
      instance: null,
      defaultSettings: {
        hideEdgesOnMove: true,
        defaultNodeColor: '#ccc'
      },
      forceAtlas2Settings: {}
    }
  };

  var edgeId = 0;

  // Event listeners
  //-----------------

  // Enter key on url input
  $('#url').keypress(function(e) {
    if (e.which !== 13)
      return;

    var url = $(this).val().trim();

    if (!url)
      return;

    // Requesting graph from api
    var data = {
      url: url
    };

    if (app.token)
      data.token = app.token;

    $.post('/graph', data, function(response) {
      if (response.error) {
        console.log(response.error, response.details);
        return;
      }

      // Setting token if this is the first API call
      if (!app.token)
        app.token = response.token;

      // Loading graph
      loadGraph(response.graph);
    });

    // Blurring input
    $(this).blur();
  });

  // On node doubleclick
  function onNodeDoubleClick(e) {
    console.log(e);
  }


  // Callbacks
  //-----------

  // When the graph is received from API
  function loadGraph(graph) {

    // Instanciating sigma for the first time
    if (!app.sigma.instance) {
      app.sigma.instance = new sigma({
        container: document.getElementById('graph'),
        settings: app.sigma.defaultSettings
      });

      // Binding events
      app.sigma.instance.bind('doubleClickNode', onNodeDoubleClick);
    }

    var s = app.sigma.instance;

    // Adding nodes and edges
    graph.nodes.forEach(function(n) {

      // Not adding if node already exists
      if (s.graph.nodes(n.id) !== undefined)
        return;

      // Casting to string id
      n.id += '';

      n.size = n.size || 1;
      s.graph.addNode(n);
    });

    graph.edges.forEach(function(e) {

      // Attributing an arbitrary id
      e.id = ''+ (edgeId++);

      // Casting to string source and target
      e.source += '';
      e.target += '';

      // Checking existence of similar edge
      if (s.graph.hasSimilarEdge(e.source, e.target))
        return;

      s.graph.addEdge(e);
    });

    // Refreshing
    s.refresh();
  }

  // Sigma's extensions
  //--------------------
  sigma.classes.graph.addMethod('hasSimilarEdge', function(s, t) {
    return !!this.allNeighborsIndex[s][t];
  });

  // Exporting to window for convenience
  this.app = app;
}).call(this);
