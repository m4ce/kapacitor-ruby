#
# client.rb
#

require 'net/http'
require 'json'

module Kapacitor
  class Client
    attr_reader :uri, :http

    def initialize(opts = {})
      host = opts['host'] || 'localhost:9092'
      version = opts['version'] || 'v1'

      @uri = URI.parse("http://#{host}/kapacitor/#{version}")
      @http = Net::HTTP.new(@uri.host, @uri.port)
    end

    def define_template(id, opts = {})
      raise ArgumentError, "Kapacitor template type is required" unless opts['type']
      raise ArgumentError, "Kapacitor template tickscript required" unless opts['script']

      if opts['type']
        raise ArgumentError, "Kapacitor template type can be either 'batch' or 'stream'" unless (opts['type'] == 'batch' or opts['type'] == 'stream')
      end

      req = {
        'id' => id,
        'type' => opts['type'],
        'script' => opts['script']
      }

      api_post('/templates', req)
    end

    def update_template(id, opts = {})
      req = {}
      req['type'] = opts['type'] if opts['type']
      req['script'] = opts['script'] if opts['script']

      if opts['type']
        raise ArgumentError, "Kapacitor template type can be either 'batch' or 'stream'" unless (opts['type'] == 'batch' or opts['type'] == 'stream')
      end

      api_patch("/templates/#{id}", req) unless req.empty?
    end

    def delete_template(id)
      api_delete("/templates/#{id}")
    end

    def templates(offset: 0, limit: 100)
      templates = []

      loop do
        res = api_get("/templates?offset=#{offset}&limit=#{limit}")['templates']
        break unless res.size > 0
        templates += res
        offset += limit
      end

      templates
    end

    def define_task(id, opts = {})
      raise ArgumentError, "Kapacitor task dbrps is required" unless opts['dbrps']

      if (opts['template_id'].nil? and opts['type'].nil? and opts['script'].nil?) or (opts['template_id'] and (opts['type'] or opts['script']))
        raise ArgumentError, "Must specify either a Template ID or a script and type"
      elsif opts['template_id'].nil? and (opts['type'].nil? or opts['script'].nil?)
        raise ArgumentError, "Must specify both task type and script when not using a Template ID"
      end

      if opts['status']
        raise ArgumentError, "Kapacitor task status can be either 'enabled' or 'disabled'" unless (opts['status'] == 'enabled' or opts['status'] == 'disabled')
      end

      if opts['type']
        raise ArgumentError, "Kapacitor task type can be either 'batch' or 'stream'" unless (opts['type'] == 'batch' or opts['type'] == 'stream')
      end

      req = {
        'id' => id,
        'dbrps' => opts['dbrps'],
        'status' => opts['status'] || 'enabled'
      }

      if opts['template_id']
        req['template-id'] = opts['template_id']
      else
        req['type'] = opts['type']
        req['script'] = opts['script']
      end

      req['vars'] = opts['vars'] if opts['vars']

      api_post('/tasks', req)
    end

    def update_task(id, opts = {})
      req = {}
      req['template-id'] = opts['template_id'] if opts['template_id']
      req['type'] = opts['type'] if opts['type']
      req['dbrps'] = opts['dbrps'] if opts['dbrps']
      req['script'] = opts['script'] if opts['script']
      req['status'] = opts['status'] if opts['status']
      req['vars'] = opts['vars'] if opts['vars']

      if opts['type']
        raise ArgumentError, "Kapacitor template type can be either 'batch' or 'stream'" unless (opts['type'] == 'batch' or opts['type'] == 'stream')
      end

      if opts['status']
        raise ArgumentError, "Kapacitor task status can be either 'enabled' or 'disabled'" unless (opts['status'] == 'enabled' or opts['status'] == 'disabled')
      end

      api_patch("/tasks/#{id}", req) unless req.empty?
    end

    def delete_task(id)
      api_delete("/tasks/#{id}")
    end

    def tasks(offset: 0, limit: 100)
      tasks = []

      loop do
        res = api_get("/tasks?fields=id&offset=#{offset}&limit=#{limit}")['tasks']
        break unless res.size > 0

        res.each do |task|
          tasks << api_get("/tasks/#{task['id']}")
        end

        offset += limit
      end

      tasks
    end

private
    def api_get(q)
      begin
        req = Net::HTTP::Get.new(self.uri.path + q, {'Content-type' => 'application/json', 'Accept' => 'application/json'})
        resp = self.http.request(req)

        if resp.code == '200'
          begin
            data = JSON.parse(resp.body)
          rescue JSON::ParserError
            raise Exception, "Failed to decode response message"
          end
        else
          raise Exception, "Query returned a non successful HTTP code (Code: #{resp.code}, Error: #{resp.message})"
        end
      rescue
        raise Exception, "Failed to execute GET request to Kapacitor REST API (#{$!})"
      end

      data
    end

    def api_post(q, data)
      begin
        req = Net::HTTP::Post.new(self.uri.path + q, {'Content-Type' => 'application/json', 'Accept' => 'application/json'})
        req.body = data.to_json
        resp = self.http.request(req)

        if resp.code == '200'
          begin
            data = JSON.parse(resp.body)
          rescue JSON::ParserError
            raise Exception, "Failed to decode response message"
          end
        else
          raise Exception, "Query returned a non successful HTTP code (Code: #{resp.code}, Error: #{resp.message})"
        end
      rescue
        raise Exception, "Failed to execute POST request to Kapacitor REST API (#{$!})"
      end

      data
    end

    def api_delete(q)
      begin
        req = Net::HTTP::Delete.new(self.uri.path + q, {'Content-type' => 'application/json', 'Accept' => 'application/json'})
        resp = self.http.request(req)

        if resp.code == '204'
          if resp.body
            begin
              data = JSON.parse(resp.body)
            rescue JSON::ParserError
              raise Exception, "Failed to decode response message"
            end
          end
        else
          raise Exception, "Query returned a non successful HTTP code (Code: #{resp.code}, Error: #{resp.message})"
        end
      rescue
        raise Exception, "Failed to execute DELETE request to Kapacitor REST API (#{$!})"
      end

      data
    end

    def api_patch(q, data)
      begin
        req = Net::HTTP::Patch.new(self.uri.path + q, {'Content-Type' => 'application/json', 'Accept' => 'application/json'})
        req.body = data.to_json
        resp = self.http.request(req)

        if resp.code == '200'
          begin
            data = JSON.parse(resp.body)
          rescue JSON::ParserError
            raise Exception, "Failed to decode response message"
          end
        else
          raise Exception, "Query returned a non successful HTTP code (Code: #{resp.code}, Error: #{resp.message})"
        end
      rescue
        raise Exception, "Failed to execute PATCH request to Kapacitor REST API (#{$!})"
      end

      data
    end
  end
end
