

class Porter:

    def __init__(self):
        """"""
        self.transferred = []
        self.stalled = []

    def transfer(self):
        raise 'Not implemented'

    def log_transfer(self, succes, filename, verbose=True):
        """store the filename in stalled or transferred report list"""
        if succes:
            self.transferred.append(filename)
            if verbose:
                print("{} is succesfully transferred "
                      "to S3 bucket".format(filename))
        else:
            self.stalled.append(filename)
            if verbose:
                print("{} is not transferred to S3 bucket".format(filename))

    def report(self, reset_file=False, transfertype="Baltrad to S3"):
        """report about the transferred and stalled files"""
        if reset_file:
            file_handler = "w"
        else:
            file_handler = "a"
        with open('./logtest', file_handler) as outfile:
            outfile.write("-" * 55 + "\n")
            outfile.write("Data transfer at {} from {}:\n".format(
                datetime.now().strftime("%Y-%m-%d %H:%M"), transfertype))
            outfile.write("-" * 55 + "\n")
            outfile.write("\n")
            outfile.write("Files not transferred:\n")
            outfile.write("\n".join(self.stalled))
            outfile.write("\n\n")
            outfile.write("Files succesfully transferred:\n")
            outfile.write("\n".join(self.transferred))
            outfile.write("\n\n\n")


class BaltradToS3(Porter):

    def __init__(self, ftp_url, ftp_login, ftp_pwd, bucketname):
        """Tunnel Baltrad server to S3

        :param ftp_URL:
        :param ftp_LOGIN:
        :param ftp_PASSWORD:
        :param bucketname:
        """
        Porter.__init__(self)
        self.ftp = BaltradFTPConnector(ftp_url, ftp_login, ftp_pwd)
        self.s3 = S3Connector(bucketname)

    def transfer(self, name_match="_vp_", overwrite=False,
                 limit=None, verbose=False):
        """transfer all current baltrad files to s3 with the given name_match
        included

        :param name_match:
        :param overwrite:
        :param limit:
        :param verbose:
        :return:
        """

        for j, filename in enumerate(self.ftp.list_files(
                namematch=name_match)):

            # get the files from ftp:
            with open(filename, 'bw') as f:
                self.ftp._ftp.retrbinary('RETR ' + filename, f.write)

                upload_succes = self.s3.upload_file_enram(filename,
                                                          overwrite=overwrite)
                self.log_transfer(upload_succes, filename, verbose)

            if isinstance(limit, int) and j >= limit-1:
                break


class LocalToS3(Porter):

    def __init__(self, bucketname, filepath):
        """Tunnel Baltrad server to S3

        :param bucketname:
        :param filepath:
        """
        Porter.__init__(self)
        self.s3 = S3Connector(bucketname)
        self.local = LocalConnector(filepath)

    def transfer(self, name_match="_vp_", overwrite=False,
                 limit=None, verbose=False):
        """transfer all profiles in folder to s3

        :param name_match:
        :param overwrite:
        :param limit:
        :param verbose:
        :return:
        """
        for j, filepath in enumerate(
                self.local.list_files(name_match, paths=True)):

            upload_succes = self.s3.upload_file_enram(filepath,
                                                      overwrite=overwrite)
            self.log_transfer(upload_succes, os.path.split(filepath)[-1],
                              verbose)

            if isinstance(limit, int) and j >= limit-1:
                break
