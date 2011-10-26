require 'minitest/spec'
require 'minitest/autorun'
require 'fileutils'

# avoid "warning: already initialized constant"
# when pulling hack ARGV = ["test-mode"]
def with_argv(*args)
  backup = ARGV.dup
  begin
    ARGV.replace(args)
    yield
  ensure
    ARGV.replace(backup)
  end
end

# orientation - parsing data is more procedural than object oriented
# to maintain simple executable, but expose its methods and objects
# for testing, execute warpt in testing mode which prevents file
# processing by source script
$LOAD_PATH.unshift(File.dirname(__FILE__))
with_argv("test-mode") do
    require 'warpt'
end


# override standard timestamp batch folder
include Config
def Config.batch_folder() batch_parent + "test_warpt_batch" end


# redirect console to avoid noise
# and allow testing of contents
class StringStream < String
  def write(message)
    self.<< message
  end
end

def capture_stdout(&test_block)
  raise "Expected block!" unless block_given?
  stdout_log = StringStream.new
  old = $stdout.clone
  $stdout = stdout_log
  test_block.call
  $stdout = old
  stdout_log
end


describe Datastore do 
  before do
    FileUtils.rm_rf Config.batch_folder

    def empty_file_path()
      app_path + '/test_files/reference_issues/empty.txt'
    end

    def unknown_file_path()
      app_path + '/test_files/reference_issues/format_unknown.txt'
    end

    def malformed_file_path()
      app_path + '/test_files/reference_issues/format_error.txt'
    end

    def invalid_name_path()
      app_path + '/test_files/reference_issues/data_issue_name.txt'
    end

    def invalid_date_path()
      app_path + '/test_files/reference_issues/data_issue_date.txt'
    end

    def reference_data_set()
      [(app_path + '/test_files/reference_data/comma.txt'),
       (app_path + '/test_files/reference_data/pipe.txt'),
       (app_path + '/test_files/reference_data/semicolon.txt')]
    end

    def reference_result_set()
      [{:reference => (app_path + '/test_files/reference_result/dob-sort.txt'),
        :generated => (batch_parent + "test_warpt_batch/results/dob-sort.txt")},
       {:reference => (app_path + '/test_files/reference_result/gender-sort.txt'),
        :generated => (batch_parent + "test_warpt_batch/results/gender-sort.txt")},
       {:reference => (app_path + '/test_files/reference_result/gender-sort.txt'),
        :generated => (batch_parent + "test_warpt_batch/results/gender-sort.txt")}]
    end

    def generated_log()
      batch_parent + "test_warpt_batch/log/warpt_log.txt"
    end
  end

  it "can handle empty files" do
    # copy empty file to data_drop folder
    file_name = File.basename(empty_file_path)
    source_path = empty_file_path
    destination_path = Config.data_path + file_name
    FileUtils.cp(source_path, destination_path)
    # run warpt to process files, capture console
    console_activity = capture_stdout{warpt_run()}
    error_expected = "Alert -- Empty File"
    wont_be_nil(console_activity.index(error_expected))
  end

  it "can handle unknown files" do
    # copy unknown file to data_drop folder
    file_name = File.basename(unknown_file_path)
    source_path = unknown_file_path
    destination_path = Config.data_path + file_name
    FileUtils.cp(source_path, destination_path)
    # run warpt to process files, capture console
    console_activity = capture_stdout{warpt_run()}
    error_expected = "ERROR -- Unknown Format"
    wont_be_nil(console_activity.index(error_expected))
  end

  it "can handle malformed files" do
    # copy malformed file to data_drop folder
    file_name = File.basename(malformed_file_path)
    source_path = malformed_file_path
    destination_path = Config.data_path + file_name
    FileUtils.cp(source_path, destination_path)
    # run warpt to process files, capture console
    console_activity = capture_stdout{warpt_run()}
    error_expected = "ERROR -- [Line: 2] -- Malformed data encountered"
    wont_be_nil(console_activity.index(error_expected))
  end

  it "can detect name data issues" do
    # copy malformed file to data_drop folder
    file_name = File.basename(invalid_name_path)
    source_path = invalid_name_path
    destination_path = Config.data_path + file_name
    FileUtils.cp(source_path, destination_path)
    # run warpt to process files, capture console
    console_activity = capture_stdout{warpt_run()}
    error_expected = "ERROR -- [Line: 2] -- Trival/Null Name value in data"
    wont_be_nil(console_activity.index(error_expected))
  end

  it "can detect date data issues" do
    # copy malformed file to data_drop folder
    file_name = File.basename(invalid_date_path)
    source_path = invalid_date_path
    destination_path = Config.data_path + file_name
    FileUtils.cp(source_path, destination_path)
    # run warpt to process files, capture console
    console_activity = capture_stdout{warpt_run()}
    error_expected = "ERROR -- [Line: 2] -- Invalid date value in data"
    wont_be_nil(console_activity.index(error_expected))
  end 

  it "can handle file system exceptions" do
    # create "staging" directory, when program attempts to create
    # existing folder, runtime exception should be reported
    Dir.mkdir(Config.batch_folder)
    Dir.mkdir(Config.log_folder)
    Dir.mkdir(Config.staging_folder)
    # copy reference files to data_drop folder
    reference_data_set.each do |file_path|
      file_name = File.basename(file_path)
      source_path = file_path
      destination_path = Config.data_path + file_name
      FileUtils.cp(source_path, destination_path)
    end
    # run warpt to process files, capture console
    console_activity = capture_stdout{warpt_run()}
    assert_match /.*ERROR -- Fatal Error.*/, console_activity
    assert_match /.*Stack Trace.*/, console_activity
  end

  it "can properly process reference data" do
    # copy reference files to data_drop folder
    reference_data_set.each do |file_path|
      file_name = File.basename(file_path)
      source_path = file_path
      destination_path = Config.data_path + file_name
      FileUtils.cp(source_path, destination_path)
    end
    # run warpt to process files, capture console
    console_activity = capture_stdout{warpt_run()}
    reference_result_set.each do |reference_record|
      generated_data = reference_record[:generated]
      reference_data = reference_record[:reference]
      # obtain generated data
      gen_out_file = File.open(generated_data)
      generated_content = gen_out_file.read
      gen_out_file.close
      # obtain reference data
      ref_out_file = File.open(reference_data)
      reference_content = ref_out_file.read
      ref_out_file.close
      # test generated output against reference
      # .. using assert_equal (over must_match) with
      # .. hope minitest diff will be invoked (though
      # .. this does not seem to be working?)
      assert_equal reference_content, generated_content
    end
    # check console for expected results
    # ... avoiding must_be_nil(console_activity.index("ERROR"))
    # ... because error messages will not be unique/informative
    refute_match /.*Alert.*/, console_activity
    refute_match /.*ERROR.*/, console_activity
    assert_match /.*Processing Complete.*/, console_activity
    # obtain log data
    log_file = File.open(generated_log)
    log_content = log_file.read
    log_file.close
    # test log wont_match "alert" or "error"
    refute_match /.*Alert.*/, log_content
    refute_match /.*ERROR.*/, log_content
    assert_match /.*Processing Complete.*/, log_content
  end
end
