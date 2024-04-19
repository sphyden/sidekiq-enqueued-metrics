require "yaml"
require "redis"
require "aws-sdk-cloudwatch"

def sscan(conn, key)
  cursor = "0"
  result = []
  loop do
    cursor, values = conn.sscan(key, cursor)
    result.push(*values)
    break if cursor == "0"
  end
  result
end

def handler(event:, context:)
  config = YAML.load_file("config.yml")
  client = Aws::CloudWatch::Client.new()

  config["sidekiq_namespaces"].each do |ns, cfg|
    host = cfg["host"]
    port = cfg["port"]
    db = cfg["db"]
    r = Redis.new(host: host, port: port, db: db)

    processes = sscan(r, "#{ns}:processes")
    queues = sscan(r, "#{ns}:queues")

    pipe2_res = r.pipelined do
      processes.each {|key| r.hget(key, "busy")}
      queues.each {|queue| r.llen("#{ns}:queue:#{queue}") }
    end
      
    s = processes.size
    enqueued = pipe2_res[s..-1].map(&:to_i).inject(0, &:+)

    puts "#{cfg["service_name"]} currently has #{enqueued} enqueued jobs"

    dimensions = [
      {
        name: "ServiceName",
        value: cfg["service_name"]
      },
      {
        name: "Environment",
        value: "production"
      }
    ]
  
    client.put_metric_data({
      namespace: "Sidekiq",
      metric_data: [
        {
          metric_name: "enqueued",
          dimensions: dimensions,
          unit: "Count",
          value: enqueued
        }
      ]
    })
  end
end
