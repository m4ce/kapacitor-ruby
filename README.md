# Ruby client library for Kapacitor JSON REST API
This is a simple Ruby client library that allows to interact with the Kapacitor JSON REST API.

Pull requests to add additional API features are very welcome. I only implemented what I needed.

## Install
To install it simply issue the following command:

```
gem install kapacitor-ruby
```

## Usage
```
require 'kapacitor/client'
kapacitor = Kapacitor::Client.new(host: 'localhost:9092', version: 'v1')
```

### Templates

#### `define_template`
Create a new template definition
```
define_template(id: 'name', type: 'stream', script: 'tickscript')
```

#### `update_template`
Update one or more templates' options
```
update_template(id: 'name', type: 'batch')
```

#### `delete_template`
Delete a template
```
delete_template(id: 'name')
```

#### `templates`
Fetch all templates
```
templates()
```

### Tasks

#### `define_task`
Create a new task
```
define_task(id: 'name', template_id: 'optional template', type: 'stream', dbrps: [{'db' => 'telegraf', 'rp' => 'default'}], script: 'tickscript', status: 'enabled', vars: {})
```

#### `update_task`
Update one or more task's options

```
update_task(id: 'name', template_id: 'optional template', type: 'stream', dbrps: [{'db' => 'telegraf', 'rp' => 'default'}], script: 'tickscript', status: 'enabled', vars: {})
```

#### `delete_task`
Delete a task
```
delete_task(id: 'name')
```

#### `tasks`
Fetch all tasks
```
tasks()
```

## Contact
Matteo Cerutti - matteo.cerutti@hotmail.co.uk
