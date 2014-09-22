package org.icgc.dcc.data.viewer.sftp;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.Resources.getResource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.sshd.SshServer;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.Session;
import org.apache.sshd.common.SshConstants;
import org.apache.sshd.common.util.Buffer;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.FileSystemFactory;
import org.apache.sshd.server.FileSystemView;
import org.apache.sshd.server.UserAuth;
import org.apache.sshd.server.auth.UserAuthNone;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.sftp.SftpSubsystem;
import org.icgc.dcc.data.common.ExportedDataFileSystem;

import com.google.common.io.Resources;
import com.google.common.util.concurrent.AbstractService;

@Slf4j
public class ExportedDataSFTPService extends AbstractService {

  private static final String README_FILE = "SFTP_README.txt";
  private SshServer sshd;
  private ExportedDataFileSystem fs;

  private final String mount;

  private final int port;

  private final int bufferSize;

  private final String currentReleaseSymlink;

  private final String README_TEXT;

  public ExportedDataSFTPService(String mount, int bufferSize, int port, String currentReleaseSymlink) {
    this.mount = mount;
    this.bufferSize = bufferSize;
    this.port = port;
    this.currentReleaseSymlink = currentReleaseSymlink;
    this.README_TEXT = getReadMe();
  }

  @Override
  protected void doStart() {
    sshd = SshServer.setUpDefaultServer();
    sshd.setPort(port);

    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(
        "hostkey.ser"));

    List<NamedFactory<UserAuth>> userAuthFactories = new ArrayList<NamedFactory<UserAuth>>();
    userAuthFactories.add(new NamedFactory<UserAuth>() {

      @Override
      public String getName() {
        return "none";
      }

      @Override
      public UserAuth create() {
        return new UserAuthNoneWithDisplay();
      }

      class UserAuthNoneWithDisplay extends UserAuthNone {

        private static final String MESSAGE_EOF = "\n\n";

        @Override
        public Boolean auth(ServerSession session, String username, Buffer buffer) {
          try {
            write(session, README_TEXT);
          } catch (Exception e) {
            log.error("Fail to display readme", e);
          }
          return true;
        }

        private void write(ServerSession session, String message) throws IOException {
          // Create message buffer
          Buffer buffer = session.createBuffer(SshConstants.Message.SSH_MSG_USERAUTH_BANNER, 0);
          buffer.putString(message);
          buffer.putString(MESSAGE_EOF);
          session.writePacket(buffer);
        }
      }

    });

    sshd.setUserAuthFactories(userAuthFactories);

    try {
      fs = new ExportedDataFileSystem(mount, bufferSize, currentReleaseSymlink);
    } catch (IOException e) {
      log.error("Fail to instantiate file system", e);
    }
    sshd.setFileSystemFactory(new FileSystemFactory() {

      @Override
      public FileSystemView createFileSystemView(Session session)
          throws IOException {
        return new ExportedDataFileSystemView(fs);
      }

    });

    List<NamedFactory<Command>> namedFactoryList = new ArrayList<NamedFactory<Command>>();
    namedFactoryList.add(new SftpSubsystem.Factory());
    sshd.setSubsystemFactories(namedFactoryList);
    try {
      sshd.start();
    } catch (IOException e) {
      log.error("Fail to start sshd", e);
    }

  }

  @Override
  protected void doStop() {
    try {

      sshd.stop(true);
      fs.close();
      notifyStopped();
    } catch (InterruptedException e) {
      log.error("Failed to stop SFTP server on {}:{} : {}", new Object[] {
          sshd.getHost(), sshd.getPort(), e.getMessage() });
      notifyFailed(e);
    } catch (IOException e) {
      log.error(
          "Failed to close the file system {}:{} : {}",
          new Object[] { sshd.getHost(), sshd.getPort(),
              e.getMessage() });
      notifyFailed(e);
    }

  }

  private static String getReadMe() {
    try {
      return Resources.toString(getResource(README_FILE), UTF_8);
    } catch (IOException e) {
      log.error("No README file found: {}", README_FILE, e);
      return "";
    }
  }
}
