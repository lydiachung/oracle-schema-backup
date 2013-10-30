
require 'open-uri'
require 'java'
require 'net/smtp'
require 'yaml'
require 'fileutils'
require 'zip/zip'

java_import 'oracle.jdbc.OracleDriver'
java_import 'java.sql.DriverManager'

class BackupManager

  def initialize()
    @c_work_dir = Dir.pwd
    @o_ser_conn = YAML.load_file('connection.yml')
    @c_ora_url = @o_ser_conn['ora_url']
    @c_ora_user = @o_ser_conn['ora_user']
    @c_ora_pass = @o_ser_conn['ora_pass']
    @c_ora_dir = @o_ser_conn['ora_dir']
    @c_ora_ser = @o_ser_conn['ora_ser']
    @c_winrar_path = @o_ser_conn['winrar_path']
    @o_conn = nil
    @n_folder_name = 0
    
    o_oradriver = OracleDriver.new()
    DriverManager.registerDriver(o_oradriver)
    @o_conn = DriverManager.get_connection(@c_ora_url, @c_ora_user, @c_ora_pass)
    @c_backup_path = get_backup_path()

  end
  
  def get_backup_path()
    c_query = %Q{select directory_path from dba_directories where directory_name = '#{@c_ora_dir}'}
    o_stmt = @o_conn.create_statement()
    o_rs = o_stmt.execute_query(c_query)
    if o_rs.next()
      c_backup_path = o_rs.get_string("directory_path")
    end
    return c_backup_path
  end
  
  def archive_dmp(l_zip_dmp)
    zip_dmp() if l_zip_dmp
    archive_zipped_dmp()
  end
  
  def schema_export()
    t_backup_date = Time.now
    c_backup_date = t_backup_date.strftime("%Y%m%d")
    c_query = %Q{select username from dba_users where account_status in ('OPEN', 'EXPIRED(GRACE)') and username not in ('SYS', 'SYSTEM') order by username} 
    o_stmt = @o_conn.create_statement()
    o_rs = o_stmt.execute_query(c_query)
    while o_rs.next()
      c_schema_name = o_rs.get_string("username")
      c_run_cmd = %Q{start /wait expdp system/#{@c_ora_pass}@#{@c_ora_ser} schemas=#{c_schema_name} directory=#{@c_ora_dir} dumpfile=#{c_schema_name}_#{c_backup_date}.dmp logfile=#{c_schema_name}_#{c_backup_date}.log}
      c_return = %x{#{c_run_cmd}}
    end
  end
  
  # zip all the dmp in the backups folder
  def zip_dmp()
    Dir.glob(File.join(@c_backup_path, "*.dmp")) do |c_file_name|  
      c_file_name = c_file_name.upcase
      c_rar_name = File.basename(c_file_name, ".DMP")
      c_rar_path = File.join(@c_backup_path, "#{c_rar_name}")
      run_cmd = %x{#{@c_winrar_path} a -ep #{c_rar_path}.rar #{c_file_name}}
    end
  end
  
  def get_archive_folder_name()
    t_archive_date = Time.now 
    c_archive_date = t_archive_date.strftime("%Y%m%d")
    c_archive_folder = File.join(@c_backup_path,"archive-#{c_archive_date}-#{@n_folder_number}")
    return c_archive_folder
  end
  
  # move zipped dmp to its folder
  def archive_zipped_dmp(c_archive_folder)
    c_archive_folder = get_archive_folder_name()
    FileUtils.mkdir_p(c_archive_folder)
    FileUtils.mv(Dir.glob(File.join(@c_backup_path, "*.rar")), c_archive_folder)
    FileUtils.mv(Dir.glob(File.join(@c_backup_path, "*.zip")), c_archive_folder)
    begin
      FileUtils.rm(Dir.glob(File.join(@c_backup_path, "*.dmp")))
    rescue Exception => e
      puts "unable to remove dmp file, #{e.message}"
    end
    begin
      FileUtils.rm(Dir.glob(File.join(@c_backup_path, "*.log")))
    rescue Exception => e
      puts "unable to remove log file, #{e.message}"
    end
  end  
end

o_bm = BackupManager.new()
o_bm.archive_dmp(false)
o_bm.archive_dmp(true)
o_bm.schema_export()
o_bm.archive_dmp(true)


