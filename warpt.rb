require 'date'
require 'fileutils'

# warpt -- wrong approach ruby parsing test
# .. files placed in data_drop folder next to program file will be parsed
# .. and moved into a directory created to logically organize data
# .. in an automated system, files would be named for filing and archiving,
# .. but this program assumes human activation, so dated folders are used

# orientation - to maintain simple executable, but expose its methods
# and objects for testing, execute warpt in testing mode which
# prevents file processing by source script
$test_mode = (ARGV[0] == "test-mode")

# centralize configuration
# .. prefer methods over inflexible constants
module Config

  def app_path()
    # beware -- vulnerable to Dir.chdir
    File.expand_path(File.dirname(__FILE__))
  end

  def data_path()
    app_path + '/data_drop/'
  end

  def batch_parent()
    app_path + '/data_batch/'
  end

  def batch_folder()
    # beware -- vulnerable to file system conflict when extending
    # .. to non-human processing (must then serialize directory
    # .. or move to microsecond tagging)
    batch_parent + Time.new.strftime("%Y%m%d_%H%M%S") + "_warpt_batch"
  end

  def data_parsed_folder()
    batch_folder + '/data_parsed/'
  end

  def data_rejected_folder()
    batch_folder  + '/data_rejected/'
  end

  def log_folder()
    batch_folder  + '/log/'
  end

  def log_path()
    log_folder + "warpt_log.txt"
  end

  def results_folder()
    batch_folder  + '/results/'
  end

  def staging_folder()
    batch_folder  + '/staging/'
  end

  def reference_data_folder()
    reference_data_folder + '/test_files/reference_data/'
  end

  def reference_result_folder()
    reference_data_folder + '/test_files/reference_result/'
  end

  def export_delimiter()
    {:delimiter => "\t", :proxy => "_"}
  end

  def delimiter_options()
    [{:format => "pipe",:delimiter => "|",:count => 6},
     {:format => "comma",:delimiter => ",",:count => 5},
     {:format => "semicolon",:delimiter => ";",:count => 6}]
  end

  def required_exports()
    [{:sort => "gender",:export_name => "gender-sort.txt",:test_label => "Output 1:"},
     {:sort => "date",:export_name => "dob-sort.txt",:test_label => "Output 2:"},
     {:sort => "name",:export_name => "reverse-name-sort.txt",:test_label => "Output 3:"}]
  end

  module_function :app_path,
                  :data_path,
                  :batch_parent,
                  :batch_folder,
                  :data_parsed_folder,
                  :data_rejected_folder,
                  :log_folder,
                  :log_path,
                  :results_folder,
                  :staging_folder,
                  :reference_data_folder,
                  :reference_result_folder,
                  :delimiter_options,
                  :required_exports
end

# Logger singleton
# .. lazy instantiation "@log ||= File.new(log_path,'w')"
# .. creates problems with testing :: closed stream
# .. thus, experiment with close-on-update log
class Logger
  include Config

  def self.create
    if !(File.file? Config.log_path) then
      if !(File.directory? Config.batch_parent) then
        Dir.mkdir(Config.batch_parent)
      end
      if !(File.directory? Config.batch_folder) then
        Dir.mkdir(Config.batch_folder)
      end
      if !(File.directory? Config.log_folder) then
        Dir.mkdir(Config.log_folder)
      end
    end
    File.open(Config.log_path, 'w') {|f| f.write("")}
  end

  def self.log(message)
    stamp = Time.new.strftime("%Y-%m-%d-%H%M%S")
    File.open(Config.log_path, 'a') {|f| f.puts(stamp + "  " + message)}
  end

  def self.empty_line()
    File.open(Config.log_path, 'a') {|f| f.puts("")}
  end

  def self.console(message)
    stamp = Time.new.strftime("%Y-%m-%d-%H%M%S")
    puts ("warpt .. " + stamp + " .. " + message)
  end
end

# data warehouse model
# .. source file data managed via instance objects
# .. aggregate data stored via class instance variable
class Datastore
  include Config
  attr_accessor :filepath, :lines, :rows, :format, :error, :description
  attr_reader :empty

  # refactor note .. description not needed?
  def initialize(file_path)
    @filepath = file_path
    @lines = []
    @empty = true
    @error = false
    @description = "none"
    @format = "unknown"
    @rows = []
  end

  def self.reset
    @warehouse = []
  end

  def self.update(data_rows)
    @warehouse.concat(data_rows)
  end

  def self.warehouse
    @warehouse
  end

  def self.export(sort_option)
    case sort_option
    when "gender"
      sorted_rows = @warehouse.sort_by {|data_row| data_row[:sort_gender]}
    when "date"
      sorted_rows = @warehouse.sort_by {|data_row| data_row[:sort_date]}
    when "name" #descending
      sorted_rows = @warehouse.sort_by {|data_row| data_row[:sort_name]}.reverse
    end
    # format export
    data_rows = []
    sorted_rows.each {|data_hash| data_rows << data_hash[:row]}
    data_export = data_rows.join("\n")
    return data_export
  end

  def readlines()
    progress_message = (" "*8) + ">> Reading File Data..."
    Logger.log(progress_message)
    # obtain file contents
    f = File.open(filepath)
    file_contents = f.read
    f.close
    # normalize eol to unix standard
    file_contents.gsub!(/\r\n?/,"\n")
    # negative "limit" parameter avoids suppression
    # of blank lines at end of file
    @lines = file_contents.split("\n",-1)
    # supress last line if blank, line created by eol
    # on final row is discarded, but if there is eol
    # on blank row, we should detect and raise flag
    if @lines.last == "" then
      @lines.pop
    end
    # determine if datastore empty
    @empty = (@lines == [])
    if @empty then
      @description = "$$$$ Alert -- "
      @description << "Empty File"
      Logger.log((" "*8) + @description)
      Logger.console(@description)
    end
  end

  def analyse_format()
    if !@empty then
      progress_message = (" "*8) + ">> Determining File Format..."
      Logger.log(progress_message)
      # check first line for known delimiters
      line = @lines[0]
      delimiter_options.each do |delimiter_record|
        delimiter = delimiter_record[:delimiter]
        if (line.count(delimiter) > 0) then
          @format = delimiter_record[:format]
          break
        end
      end
      if (@format == "unknown") then
        @error = true
        @description = "$$$$ ERROR -- "
        @description << "Unknown Format"
        Logger.log((" "*8) + @description)
        Logger.console(@description)
      end
    end
  end

  def obtain_format()
    delimiter_options.each do |delimiter_record|
      if delimiter_record[:format] == @format then
        return delimiter_record
      end
    end
  end

  def validate_format()
    if !(@error || @empty) then
      progress_message = (" "*8) + ">> Validating File Format..."
      Logger.log(progress_message)
      # validate entire file against format
      delimiter_record = obtain_format()
      delimiter = delimiter_record[:delimiter]
      delimiter_count = delimiter_record[:count]
      @lines.each_with_index do |line,index|
        if !(line.count(delimiter) == delimiter_count) then
          @error = true
          @description = "$$$$ ERROR -- "
          @description << "[Line: " + (index + 1).to_s + "] -- "
          @description << "Malformed data encountered"
          Logger.log((" "*8) + @description)
          Logger.console(@description)
          break
        end
      end
    end
  end

  def validate_data(data,index)
    # validate name
    if ((data[:surname] + data[:first_name]).length < 3) then
      @error = true
      @description = "$$$$ ERROR -- "
      @description << "[Line: " + (index + 1).to_s + "] -- "
      @description << "Trival/Null Name value in data"
      Logger.log((" "*8) + @description)
      Logger.console(@description)
    end
    # validate date
    date_string = data[:birth_date]
    month,day,year = date_string.split("/",-1)
    begin
      row_date = Date.parse(year + "-" + month + "-" + day)
    rescue ArgumentError
      @error = true
      @description = "$$$$ ERROR -- "
      @description << "[Line: " + (index + 1).to_s + "] -- "
      @description << "Invalid date value in data"
      #
      Logger.log((" "*8) + @description)
      Logger.console(@description)
    end
  end

  def parse_data()
    if !(@error || @empty) then
      progress_message = (" "*8) + ">> Parsing File Data..."
      Logger.log(progress_message)
      # parse data from each line and process
      @lines.each_with_index do |line,index|
        delimiter_record = obtain_format()
        delimiter = delimiter_record[:delimiter]
        # initialize row values
        lname = ""
        fname = ""
        minitial = ""
        gender = ""
        color = ""
        sport = ""
        dob = ""
        # parse data row
        case @format
        when "pipe"
          #Lifeson|Alex||M|8/27/1953|PANTONE 18-5424|Golf
          lname,fname,minitial,gender,dob,color,sport = line.split(delimiter,-1)
        when "comma"
          #van Giersbergen,Anneke,Female,Roller Derby,#FF00FF,03/08/1973
          lname,fname,gender,sport,color,dob = line.split(delimiter,-1)
        when "semicolon"
          #Kournikova; Anna; F; 6-3-1975; Female; purple; tennis
          lname,fname,minitial,dob,gender,color,sport = line.split(delimiter,-1)
        end
        # strip string values
        lname = lname.strip
        fname = fname.strip
        minitial = minitial.strip
        gender = parse_gender(gender.strip)
        color = color.strip
        sport = sport.strip
        dob = parse_date(dob.strip)
        # remove export delimiter
        export_delimiter = export_delimiter()[:delimiter]
        if !(export_delimiter == delimiter) then
          proxy = export_delimiter()[:proxy]
          lname.gsub!(export_delimiter,proxy)
          fname.gsub!(export_delimiter,proxy)
          minitial.gsub!(export_delimiter,proxy)
          gender.gsub!(export_delimiter,proxy)
          color.gsub!(export_delimiter,proxy)
          sport.gsub!(export_delimiter,proxy)
          dob.gsub!(export_delimiter,proxy)
        end
        # pack data record
        data_record = {:surname => lname,
                       :first_name => fname,
                       :middle_initial => minitial,
                       :gender => gender,
                       :fav_color => color,
                       :fav_sport => sport,
                       :birth_date => dob}
        # validate values
        validate_data(data_record,index)
        # manage silo data queue
        if !(@error) then
          row_object = format_data(data_record)
          @rows << row_object
        else
          break
        end
      end
    end
  end

  def parse_gender(gender)
    # convert gender to required format
    case gender
    when "M"
      gender = "Male"
    when "F"
      gender = "Female"
    end
    gender
  end

  def parse_date(date_string)
    # convert date to required format
    date_string.gsub!(/-/,"/")
    month,day,year = date_string.split("/",-1)
    month = ("0" + month)[-2..2]
    day = ("0" + day)[-2..2]
    return (month + "/" + day + "/" + year)
  end

  def format_data(data_record)
    # obtain relevant data
    last_name = data_record[:surname]
    first_name = data_record[:first_name]
    gender = data_record[:gender]
    date_of_birth = data_record[:birth_date]
    favorite_sport = data_record[:fav_sport]
    favorite_color = data_record[:fav_color]
    # format row in export format
    delimiter = export_delimiter()[:delimiter]
    data_row = last_name + delimiter
    data_row << first_name + delimiter
    data_row << gender + delimiter
    data_row << date_of_birth + delimiter
    data_row << favorite_sport + delimiter
    data_row << favorite_color
    # generic sort values
    sort_name = (last_name + "-" + first_name).downcase
    month,day,year = date_of_birth.split("/",-1)
    sort_date = year + month + day
    # gender-name sort key
    # [females before males][last name ascending]
    case gender
    when "Female"
      gender_sort_key = "0-"
    when "Male"
      gender_sort_key = "1-"
    else
      gender_sort_key = "2-"
    end
    gender_sort_key << sort_name
    # birth date (add name to make deterministic)
    date_sort_key = sort_date + "-" + sort_name
    # last name (add date to make deterministic)
    name_sort_key = sort_name + "-" + sort_date
    # pack results
    return {:row => data_row,
            :sort_gender => gender_sort_key,
            :sort_date => date_sort_key,
            :sort_name => name_sort_key}
  end

  def resolve()
    progress_message = (" "*8) + ">> Routing Processed File..."
    Logger.log(progress_message)
    if !(@error) then
      # add parsed data to warehouse
      self.class.update(@rows)
      # route file to processed folder
      file_name = File.basename(@filepath)
      source_path = Config.staging_folder + file_name
      destination_path = Config.data_parsed_folder + file_name
      FileUtils.mv(source_path, destination_path)
    else
      # route file to error folder
      file_name = File.basename(@filepath)
      source_path = Config.staging_folder + file_name
      destination_path = Config.data_rejected_folder + file_name
      FileUtils.mv(source_path, destination_path)
    end
  end
end

def warpt_run()
  Logger.create
  Logger.log("warpt Script Launch")
  Logger.console("Script Launch")
  Logger.empty_line
  # initialize datawarehouse
  Datastore.reset
  # wrap main logic in begin-rescue to catch
  # file io and unanticipated exceptions
  begin
    # check drop folder for source files
    data_files = Dir[Config.data_path + '*.{TXT,txt}']
    parsing_required = (data_files.count > 0)
    log_message = "Pending Files Count => " + data_files.count.to_s + "\n"
    if (parsing_required) then
      file_list = data_files.map{|file_path| ((" "*22) + file_path)}
      log_message << file_list.join("\n")
      log_message << "\n"
    end
    Logger.log(log_message)
  
    if (parsing_required) then
      # create required folders
      Logger.log("Creating Batch Processing Directory\n")
      if !(File.directory? Config.batch_parent) then
        Dir.mkdir(Config.batch_parent)
      end
      if !(File.directory? Config.batch_folder) then
        Dir.mkdir(Config.batch_folder)
      end
      if !(File.directory? Config.log_folder) then
        Dir.mkdir(Config.log_folder)
      end
      Dir.mkdir(Config.data_parsed_folder)
      Dir.mkdir(Config.data_rejected_folder)
      Dir.mkdir(Config.results_folder)
      Dir.mkdir(Config.staging_folder)

      # move files to staging
      Logger.log("Moving Data To Staging Folder\n")
      stage_files = []
      data_files.each do |file|
        file_name = File.basename(file)
        source_path = Config.data_path + file_name
        destination_path = Config.staging_folder + file_name
        FileUtils.mv(source_path, destination_path)
        stage_files << destination_path
      end

      # process each source data file
      Logger.log("Processing Data Batch...\n")
      file_count = stage_files.count
      stage_files.each_with_index do |file,index|
        progress_cue = "[" + (index + 1).to_s + "/" + file_count.to_s + "]"
        progress_message = "Processing File #{progress_cue} :: " + File.basename(file)
        Logger.log(progress_message)
        Logger.console(progress_message)
        silo = Datastore.new(file)
        silo.readlines
        silo.analyse_format
        silo.validate_format
        silo.parse_data
        silo.resolve
      end

      # generate required exports
      Logger.log("Exporting Required Data\n")
      export_content = ""
      Config.required_exports.each_with_index do |export_record,index|
        required_sort = export_record[:sort]
        required_filename = export_record[:export_name]
        required_label = export_record[:test_label]
        # export files
        sorted_content = Datastore.export(required_sort)
        export_content = sorted_content
        export_path = Config.results_folder + required_filename
        File.open(export_path, "w") do |export_file|
          export_file.write export_content
        end
      end
    end
  rescue => warpt_error    
    # while this may seem "cheap", the idea is to make sure
    # .. we are alerting users to unanticipated exceptions
    # .. and while it would be awesome to wrap every
    # .. vulnerable block, over years of operation, someone
    # .. will likely be forced to revisit and add rescuing
    # add debugging info to log
    log_message = "ERROR -- Fatal Error Encountered\n"
    log_message << (" "*8) + "[Error Backtrace]\n"
    log_message << (" "*8) + warpt_error.backtrace.join("\n" + (" "*8)) + "\n"
    log_message << (" "*8) + "[Error Message]\n"
    log_message << (" "*8) + warpt_error.message + "\n"
    Logger.empty_line
    Logger.log(log_message)
    # output debugging info to console
    Logger.console("ERROR -- Fatal Error -- " + warpt_error.message)
    Logger.console("[Stack Trace]")
    warpt_error.backtrace.each {|line| Logger.console(line)}
  end

  Logger.empty_line
  Logger.log("warpt Processing Complete")
  Logger.console("Processing Complete")
end

if !$test_mode then
  warpt_run()
end
