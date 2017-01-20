#
# client.rb
#
# Author: Matteo Cerutti <matteo.cerutti@hotmail.co.uk>
#

require 'net/http'
require 'json'

module Kapacitor
  class Client
    attr_reader :uri, :http

    def initialize(host: 'localhost:9092', version: 'v1')
      @uri = URI.parse("http://#{host}/kapacitor/#{version}")
      @http = Net::HTTP.new(@uri.host, @uri.port)
    end

    def define_template(id:, type:, script:)
      req = {
        'id' => id,
        'type' => type,
        'script' => script
      }

      api_post('/templates', req)
    end

    def update_template(id:, type: nil, script: nil)
      req = {}
      req['type'] = type if type
      req['script'] = script if script

      api_patch("/templates/#{id}", req) unless req.empty?
    end

    def delete_template(id:)
      api_delete("/templates/#{id}")
    end

    def templates
      api_get('/templates')['templates']
    end

    def define_task(id:, template_id: nil, type: nil, dbrps: , script: nil, status: 'enabled', vars: nil)
      if (template_id.nil? and type.nil? and script.nil?) or (template_id and (type or script))
        raise ArgumentError, "Must specify either a Template ID or a script and type"
      elsif template_id.nil? and (type.nil? or script.nil?)
        raise ArgumentError, "Must specify both task type and script when not using a Template ID"
      end

      req = {
        'id' => id,
        'dbrps' => dbrps,
        'status' => status
      }

      if template_id
        req['template-id'] = template_id
      else
        req['type'] = type
        req['script'] = script
      end

      req['vars'] = vars if vars

      api_post('/tasks', req)
    end

    def update_task(id:, template_id: nil, type: nil, dbrps: nil, script: nil, status: nil, vars: nil)
      req = {}
      req['template-id'] = template_id if template_id
      req['type'] = type if type
      req['dbrps'] = dbrps if dbrps
      req['script'] = script if script
      req['status'] = status if status
      req['vars'] = vars if vars

      api_patch("/tasks/#{id}", req) unless req.empty?
    end

    def delete_task(id:)
      api_delete("/tasks/#{id}")
    end

    def tasks
      tasks = []
      api_get('/tasks?fields=id')['tasks'].each do |id|
        tasks << api_get("/tasks/#{id}")['tasks'].each do |id|
      end
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
