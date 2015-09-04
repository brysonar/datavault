package org.datavaultplatform.common.storage.impl;

import org.datavaultplatform.common.storage.Device;
import org.datavaultplatform.common.storage.Auth;
import org.datavaultplatform.common.model.FileInfo;
import org.datavaultplatform.common.io.Progress;

import java.io.File;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Vector;

import com.jcraft.jsch.*;
import java.io.FileInputStream;

public class SFTPFileSystem extends Device {

    private String host = null;
    private String rootPath = null;    
    private Session session = null;
    private ChannelSftp channelSftp = null;
    private final int port = 22;
    private final String PATH_SEPARATOR = "/";
    
    public SFTPFileSystem(String name, Auth auth, Map<String,String> config) throws Exception {
        super(name, auth, config);
        
        // Unpack the config parameters (in an implementation-specific way)
        host = config.get("host");
        rootPath = config.get("rootPath");
    }
    
    private void Connect() throws Exception {
        
        JSch jsch = new JSch();
        session = jsch.getSession(auth.getUsername(), host, port);
        session.setPassword(auth.getPassword());
        java.util.Properties properties = new java.util.Properties();
        properties.put("StrictHostKeyChecking", "no");
        session.setConfig(properties);
        session.connect();
        
        // Start a channel for SFTP
        Channel channel = session.openChannel("sftp");
        channel.connect();
        channelSftp = (ChannelSftp)channel;
        channelSftp.cd(rootPath);
    }
    
    private void Disconnect() {
        if (channelSftp != null) {
            channelSftp.exit();
        }
        
        if (session != null) {
            session.disconnect();
        }
    }
    
    private String runCommand(String command) throws Exception {
        
        ChannelExec channelExec = null;
        
        try {
            Connect();
            
            // Start a channel for commands
            channelExec = (ChannelExec)(session.openChannel("exec"));
            
            channelExec.setCommand(command);
            channelExec.setInputStream(null);
            channelExec.setErrStream(System.err);
            InputStream in = channelExec.getInputStream();
            
            channelExec.connect();
            
            byte[] tmp = new byte[1024];
            StringBuilder response = new StringBuilder();
            
            while(true) {
                while(in.available() > 0) {
                int i = in.read(tmp, 0, 1024);
                if (i < 0) {
                    break;
                }
                response.append(new String(tmp, 0, i));
            }
        
            if (channelExec.isClosed()) {
                if (in.available() > 0) continue; 
                    System.out.println("exit-status: " + channelExec.getExitStatus());
                    break;
                }
            }
            
            System.out.println("response: " + response);
            return response.toString();
            
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (channelExec != null) {
                channelExec.disconnect();
            }
            Disconnect();
        }
    }
    
    @Override
    public List<FileInfo> list(String path) {
        
        ArrayList<FileInfo> files = new ArrayList<>();
        
        try {
            Connect();
            
            Vector filelist = channelSftp.ls(rootPath + PATH_SEPARATOR + path);
            
            for (int i = 0; i < filelist.size(); i++) {
                ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry)filelist.get(i);
                
                if (entry.getFilename().equals(".") ||
                        entry.getFilename().equals("..")) {
                        continue;
                }
                
                String entryKey = path + PATH_SEPARATOR + entry.getFilename();
                
                FileInfo info = new FileInfo(entryKey,
                                             "", // Absolute path - unused?
                                             entry.getFilename(),
                                             entry.getAttrs().isDir());
                files.add(info);
                
                // Other useful properties:
                // entry.getAttrs().getSize()
                // entry.getAttrs().isDir()
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Disconnect();
        }
        
        return files;
    }
    
    @Override
    public boolean valid(String path) {
        /* Unimplemented */
        return true;
    }

    @Override
    public boolean exists(String path) {
        
        // TODO: handle symbolic links
        
        try {
            Connect();
            
            SftpATTRS attrs = channelSftp.stat(rootPath + PATH_SEPARATOR + path);
            return true;
            
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            Disconnect();
        }
    }
    
    @Override
    public long getSize(String path) throws Exception {
        
        // TODO: handle directories and special cases
        
        try {
            Connect();
            
            SftpATTRS attrs = channelSftp.stat(rootPath + PATH_SEPARATOR + path);
            return attrs.getSize();
            
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            Disconnect();
        }
    }

    @Override
    public boolean isDirectory(String path) throws Exception {
        try {
            Connect();
            
            SftpATTRS attrs = channelSftp.stat(rootPath + PATH_SEPARATOR + path);
            return attrs.isDir();
            
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            Disconnect();
        }
    }
    
    @Override
    public String getName(String path) {
        if (path.contains(PATH_SEPARATOR)) {
            return path.substring(path.lastIndexOf(PATH_SEPARATOR) + 1);
        } else {
            return path;
        }
    }
    
    @Override
    public long getUsableSpace() throws Exception {
        try {
            Connect();
            
            // Requires the "statvfs" extension e.g. OpenSSL
            // getAvailForNonRoot() returns a count of 1k blocks
            // TODO: is this working correctly with different fragment sizes?
            // TODO: how widely supported is this extension?
            SftpStatVFS statVFS = channelSftp.statVFS(rootPath);
            return statVFS.getAvailForNonRoot() * 1024; // bytes
            
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            Disconnect();
        }
    }

    @Override
    public void copyToWorkingSpace(String path, File working, Progress progress) throws Exception {
        
        // TODO: handle directories and failures
        
        // Strip any leading separators (we want a path relative to the current dir)
        while (path.startsWith(PATH_SEPARATOR)) {
            path = path.replaceFirst(PATH_SEPARATOR, "");
        }
        
        try {
            Connect();
            
            byte[] buffer = new byte[1024 * 1024];
            BufferedInputStream bis = new BufferedInputStream(channelSftp.get(path));
            
            OutputStream os = new FileOutputStream(working);
            BufferedOutputStream bos = new BufferedOutputStream(os);
            
            int count;
            while((count = bis.read(buffer)) > 0) {
                bos.write(buffer, 0, count);
                progress.byteCount += count;
                progress.timestamp = System.currentTimeMillis();
            }
            
            bis.close();
            bos.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            Disconnect();
        }
    }

    @Override
    public void copyFromWorkingSpace(String path, File working, Progress progress) throws Exception {
        
        // Strip any leading separators (we want a path relative to the current dir)
        while (path.startsWith(PATH_SEPARATOR)) {
            path = path.replaceFirst(PATH_SEPARATOR, "");
        }
        
        // Append the archive file name (e.g. tar file)
        path = path + PATH_SEPARATOR + working.getName();
        
        try {
            Connect();
            
            byte[] buffer = new byte[1024 * 1024];
            FileInputStream fis = new FileInputStream(working);
            BufferedInputStream bis = new BufferedInputStream(fis);
            
            OutputStream os = channelSftp.put(path);
            BufferedOutputStream bos = new BufferedOutputStream(os);
            
            int count;
            while((count = bis.read(buffer)) > 0) {
                bos.write(buffer, 0, count);
                progress.byteCount += count;
                progress.timestamp = System.currentTimeMillis();
            }
            
            bis.close();
            bos.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            Disconnect();
        }
    }
}