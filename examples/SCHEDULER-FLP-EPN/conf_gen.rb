#!/usr/bin/env ruby


require 'pp'
require 'json'
require 'io/console'
require 'shellwords'

def usage(fail)
  usage = %{
Usage:
  conf_gen.rb <num-FLP> <num-EPN> <node_list>

  NOTE:
  1. <node_list> must contain at least 3 nodes
  2. env var TEST_ROOT_DIR : full path to the programs
  3. env var TEST_CONF_DIR : full path where to save conf files
  4. env var TEST_NUM_SLOTS : num of TF slots at the epns
  5. env var TEST_AMOUNT_EPN : amount of TF in the schedule
  6. env var TEST_WALL_TIME : run the code for this many minutes

  Examples:
  $ conf_gen.rb 20 10 pn02,pn04,pn05,pn06,pn07,pn08,pn10,pn11,pn12,pn13,pn15,pn16

  To expand a compressed node list format of slurm, e.g.:
  $ conf_gen.rb 20 10 $(scontrol show hostname pn[02,04-08,10-13,15-40] | paste -d, -s)
}
  puts usage
  exit 1 if fail
end


usage(true) if ARGV[0].nil? or ARGV[0].length==0 or ARGV[1].nil? or ARGV[2].nil? or
              ENV['TEST_ROOT_DIR'].nil? or ENV['TEST_CONF_DIR'].nil? or ENV['TEST_NUM_SLOTS'].nil? or
              ENV['TEST_AMOUNT_EPN'].nil? or ENV['TEST_WALL_TIME'].nil?


NUM_FLP = ARGV[0].to_i
NUM_EPN = ARGV[1].to_i
NODES = ARGV[2].split(',')
NUM_NODES = NODES.length

usage(true) if NODES.length < 3 or NUM_FLP <= 0 or NUM_EPN <= 0


puts "Num of FLPs: " + NUM_FLP.to_s
puts "Num of EPNs: " + NUM_EPN.to_s
puts "Nodes: " + NODES.join(', ')

# 1st node is scheduler!
# the rest of the nodes are split between FLPs and EPNs

SCHED_NODE = NODES[0]
NUM_FLP_NODES = [NUM_FLP, ((NUM_FLP.to_f) / (NUM_FLP + NUM_EPN).to_f * (NUM_NODES - 1).to_f).to_i].min
NUM_EPN_NODES = [NUM_EPN, NUM_NODES - 1 - NUM_FLP_NODES].min

FLP_NODES = NODES[1...(1+NUM_FLP_NODES)]
EPN_NODES = NODES[(1+NUM_FLP_NODES)...(1+NUM_FLP_NODES+NUM_EPN_NODES)]

puts "Scheduler node: " + SCHED_NODE
puts "Num of FLP nodes: " + NUM_FLP_NODES.to_s + " : " + FLP_NODES.join(', ')
puts "Num of EPN nodes: " + NUM_EPN_NODES.to_s + " : " + EPN_NODES.join(', ')

SOCKET_HASH =
{
  :type => "pair", # "push"/"pull", "pair"
  :method => "", # "bind" or "connect"
  :address => "", #"tcp://localhost:5556",
  :sndBufSize => 1000,
  :rcvBufSize => 1000,
  :rateLogging => 0
}

CHAN_HASH =
{
  :name => "",
  :sockets => [ ]
}

DEVICE_HASH =
{
  :id => "",
  :channels => [ ]
}


###################
SCHED_FLP_PORT = 40000
SCHED_EPN_PORT = 50000
sched_flp_port = SCHED_FLP_PORT
sched_epn_port = SCHED_EPN_PORT
#  sched channel
sched_dev = Marshal.load(Marshal.dump(DEVICE_HASH))
sched_dev[:id] = "scheduler"


sched_flp_chan = Marshal.load(Marshal.dump(CHAN_HASH))
sched_flp_chan[:name] = "schedflp"

NUM_FLP.times do | flp |
  sched_socket = Marshal.load(Marshal.dump(SOCKET_HASH))
  sched_socket[:type] = "push"
  sched_socket[:method] = "bind"
  sched_socket[:address] = "tcp://*:#{sched_flp_port}"
  sched_flp_port += 1

  sched_flp_chan[:sockets] << sched_socket
end
sched_dev[:channels] << sched_flp_chan

# sched epn channel
sched_epn_chan = Marshal.load(Marshal.dump(CHAN_HASH))
sched_epn_chan[:name] = "epnsched"

NUM_EPN.times do | epn |
  sched_socket = Marshal.load(Marshal.dump(SOCKET_HASH))
  sched_socket[:type] = "pull"
  sched_socket[:method] = "bind"
  sched_socket[:address] = "tcp://*:#{sched_epn_port}"
  sched_epn_port += 1

  sched_epn_chan[:sockets] << sched_socket
end
sched_dev[:channels] << sched_epn_chan



### EPNS
###################
FLP_DATA_PORT = 50000
flp_data_p = FLP_DATA_PORT
epn_config = []
epn_node_map = {}
epn_port_per_node = {}
# create all flp devices
NUM_EPN.times do | epn |
  epn_node = EPN_NODES[epn % NUM_EPN_NODES]
  epn_node_map[epn] = { :node => epn_node, :data_ports => [] }


  epn_port_per_node[epn_node] = FLP_DATA_PORT if epn_port_per_node[epn_node].nil?

  # init used port for this node
  # flp_port_per_node[flp_node] = FLP_DATA_PORT if flp_port_per_node[flp_node].nil?

  epn_dev = Marshal.load(Marshal.dump(DEVICE_HASH))

  epn_dev[:id] = "epn#{epn}"

  # data channel for the current flp
  epn_data_chan = Marshal.load(Marshal.dump(CHAN_HASH))
  epn_data_chan[:name] = "data"
  # flp sockets all do bind
  NUM_FLP.times do | flp |
    flp_socket = Marshal.load(Marshal.dump(SOCKET_HASH))
    flp_socket[:type] = "pull"
    flp_socket[:method] = "bind"
    flp_socket[:address] = "tcp://*:#{epn_port_per_node[epn_node]}"
    epn_node_map[epn][:data_ports] << epn_port_per_node[epn_node]
    epn_port_per_node[epn_node] += 1

    epn_data_chan[:sockets] << flp_socket
  end
  epn_dev[:channels] << epn_data_chan

  #  sched channel
  epn_sched_chan = Marshal.load(Marshal.dump(CHAN_HASH))
  epn_sched_chan[:name] = "epnsched"
  epn_sched_socket = Marshal.load(Marshal.dump(SOCKET_HASH))
  epn_sched_socket[:type] = "push"
  epn_sched_socket[:method] = "connect"
  epn_sched_socket[:address] = "tcp://#{SCHED_NODE}ib0:#{SCHED_EPN_PORT + epn}"

  epn_sched_chan[:sockets] << epn_sched_socket
  epn_dev[:channels] << epn_sched_chan

  epn_config << epn_dev
end




###################
flp_port_per_node = {} # track used data ports on each flp node
flp_node_map = {}

flp_config = []
# create all flp devices
NUM_FLP.times do | flp |
  flp_node = FLP_NODES[flp % NUM_FLP_NODES]
  flp_node_map[flp] = { :node => flp_node, :data_ports => [] }

  # init used port for this node
  flp_port_per_node[flp_node] = FLP_DATA_PORT if flp_port_per_node[flp_node].nil?

  flp_dev = Marshal.load(Marshal.dump(DEVICE_HASH))

  flp_dev[:id] = "flp#{flp}"

  # data channel for the current flp
  flp_data_chan = Marshal.load(Marshal.dump(CHAN_HASH))
  flp_data_chan[:name] = "data"
  # flp sockets all do bind
  NUM_EPN.times do | epn |
    epn_socket = Marshal.load(Marshal.dump(SOCKET_HASH))
    epn_socket[:type] = "push"
    epn_socket[:method] = "connect"
    epn_socket[:address] = "tcp://#{epn_node_map[epn][:node]}ib0:#{epn_node_map[epn][:data_ports][flp]}"
    flp_node_map[flp][:data_ports] << flp_port_per_node[flp_node]
    flp_port_per_node[flp_node] += 1

    flp_data_chan[:sockets] << epn_socket
  end
  flp_dev[:channels] << flp_data_chan

  #  sched channel
  flp_sched_chan = Marshal.load(Marshal.dump(CHAN_HASH))
  flp_sched_chan[:name] = "schedflp"
  flp_sched_socket = Marshal.load(Marshal.dump(SOCKET_HASH))
  flp_sched_socket[:type] = "pull"
  flp_sched_socket[:method] = "connect"
  flp_sched_socket[:address] = "tcp://#{SCHED_NODE}ib0:#{SCHED_FLP_PORT + flp}"

  flp_sched_chan[:sockets] << flp_sched_socket
  flp_dev[:channels] << flp_sched_chan

  flp_config << flp_dev
end







FAIRMQ_DEVS =
{
  :fairMQOptions =>
  {
    :devices => [ sched_dev, flp_config, epn_config ].flatten
  }
}


spm_file_lines = [ ]
# spm file
sched_spm = [ SCHED_NODE, ":", "/bin/bash -c \"",
  [ "#{ENV['TEST_ROOT_DIR']}/fairmq-ex-SCHEDULER-FLP-EPN-scheduler",
    "--id", "scheduler",
    "--amountEPNs", ENV['TEST_AMOUNT_EPN'],
    "--numEPNS", "#{NUM_EPN}",
    "--numFLPS", "#{NUM_FLP}",
    "--programTime", ENV['TEST_WALL_TIME'],
    "--mq-config", "#{ENV['TEST_CONF_DIR']}/json_conf_#{NUM_FLP}_#{NUM_EPN}.json",
    "--io-threads", "16",
    "--network-interface", "ib0",
    "--control", "static"
  ].shelljoin, "2>&1 | tee sched_log_#{NUM_FLP}_#{NUM_EPN}.log\"" ].join(' ')

spm_file_lines << sched_spm
spm_file_lines << ""

NUM_FLP.times do | flp |
  flp_spm = [ flp_node_map[flp][:node] , ":", "/bin/bash -c \"",
    [ "#{ENV['TEST_ROOT_DIR']}/fairmq-ex-SCHEDULER-FLP-EPN-flp",
      "--id", "flp#{flp}",
      "--myId", "#{flp + 1}",
      "--socket", "#{flp}",
      "--amountEPNs", ENV['TEST_AMOUNT_EPN'],
      "--numEPNS", "#{NUM_EPN}",
      "--numFLPS", "#{NUM_FLP}",
      "--programTime", ENV['TEST_WALL_TIME'],
      "--mq-config", "#{ENV['TEST_CONF_DIR']}/json_conf_#{NUM_FLP}_#{NUM_EPN}.json",
      "--io-threads", "8",
      "--network-interface", "ib0",
      "--control", "static"
    ].shelljoin, "2>&1 | tee flp_log_#{NUM_FLP}_#{NUM_EPN}_flpid#{flp}.log\""
  ].join(' ')

  spm_file_lines << flp_spm
end
spm_file_lines << ""

NUM_EPN.times do | epn |
  epn_spm = [ epn_node_map[epn][:node] , ":", "/bin/bash -c \"",
    [ "#{ENV['TEST_ROOT_DIR']}/fairmq-ex-SCHEDULER-FLP-EPN-epn",
      "--id", "epn#{epn}",
      "--myId", "#{epn + 1}",
      "--maxSlots", "#{ENV['TEST_NUM_SLOTS']}",
      "--numEPNS", "#{NUM_EPN}",
      "--numFLPS", "#{NUM_FLP}",
      "--programTime", ENV['TEST_WALL_TIME'],
      "--mq-config", "#{ENV['TEST_CONF_DIR']}/json_conf_#{NUM_FLP}_#{NUM_EPN}.json",
      "--io-threads", "8",
      "--network-interface", "ib0",
      "--control", "static"
    ].shelljoin, "2>&1 | tee epn_log_#{NUM_FLP}_#{NUM_EPN}_epnid#{epn}.log\""
  ].join(' ')
  spm_file_lines << epn_spm
end


# pp flp_config
# pp epn_config

File.open("#{ENV['TEST_CONF_DIR']}/json_conf_#{NUM_FLP}_#{NUM_EPN}.json", "w") do |f|
  f.puts JSON.pretty_generate(FAIRMQ_DEVS)
end

File.open("#{ENV['TEST_CONF_DIR']}/spm_conf_#{NUM_FLP}_#{NUM_EPN}.spm", "w") do |f|
  f.puts spm_file_lines.join("\n")
end

