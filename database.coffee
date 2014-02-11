mongo = require 'mongodb'
_ = (require 'underscore')._

ObjectID = mongo.ObjectID

collectionsList = []
collections = {}
models = {}
db = null

# Create Collections
createCollection = (n, cb) ->
 if n >= collectionsList.length
  cb()
  return

 name = collectionsList[n]

 db.collection name, (err, collection) =>
  collections[name] = collection
  createCollection n + 1, cb

# Setup database
setup = (database, _models, callback) ->
 collectionsList = (c for c of _models)
 console.log collectionsList
 for c in collectionsList
  collections[c] = null
 models = _models

 db = new mongo.Db(
  database
  new mongo.Server('localhost', 27017, {})
  {safe: true}
 )
 db.addListener 'error', onError

 db.open (pDb) ->
  createCollection 0, () ->
   callback db

onError = (err) ->
 console.log "Mongo Error: #{err}"

#Load a model
load = (type, obj, cb) ->
 collections[type].find obj, (err, cursor) ->
  cursor.toArray (err, dbObjs) ->
   objs = []
   for dbObj in dbObjs
    #console.log dbObj
    #console.log models[dbObj.type]
    if dbObj.type of models
     o = new models[dbObj.type] dbObj
     objs.push o
    else
     throw "Unidentified model: "
     console.trace obObj

   cb err, objs

getCollection = (type) -> collections[type]

bulks = {}

# Model base class
class Model
 constructor: (obj) ->
  @initialise(obj)

 _defaults:
  _id: null
  id: null

 type: "Model"

 @defaults: (defaults) ->
  @::_defaults = _.clone @::_defaults
  @::_defaults.type = @::type
  for k, v of defaults
   @::_defaults[k] = v

 initialise: (obj) ->
  @values = {}
  for k, v of @_defaults
   @values[k] = v
  @set obj

  if @isNew()
   _id = new ObjectID()
   @set _id: _id, id: _id.toHexString()

 set: (obj) ->
  for k, v of obj
   if k of @_defaults
    @values[k] = v

  if 'id' of obj and obj.id?
   @values._id = ObjectID.createFromHexString obj.id

 get: (key) ->
  @values[key]

 isNew: () ->
  if @values._id?
   return false
  else
   return true

 save: (cb) ->
  collections[@get('type')].save @toJSON(), (err, obj) =>
   cb?()

 insert: (cb) ->
  collections[@get('type')].insert @toJSON(), (err, obj) =>
   cb?()

 bulkInsert: (cb) ->
  type = @get('type')
  bulks[type] = [] unless bulks[type]
  bulks[type].push @toJSON()
  cb?()


 bulkInsertFlush: (cb) ->
  type = @get('type')
  if bulks[type]? and bulks[type].length > 0
   collections[type].insert bulks[type], (err, obj) =>
    cb?()
   bulks[type] = []
  else
   cb?()

 toJSON: () ->
  r = _.clone @values
  if r._id?
   r.id = r._id.toHexString()
  return r

exports.setup = setup
exports.load = load
exports.Model = Model
exports.getCollection = getCollection

