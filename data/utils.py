
import errno
import os
import urllib
import numpy as np
import tempfile
import zipfile
import pandas as pd
from pathlib import Path

TEMP = tempfile.gettempdir()

def _makedirs(path):
    """

    :param path: the path for creating a directionary
    :return: creating a directionary as path, if already exist then not creating
    """
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def _fetch_remote_data(remote, download_dir, data_home):
    """Download a remote datasets.
    Parameters
    ----------
    remote : Dataset
        Named tuple containing remote datasets meta information: dataset name, dataset filename,
        url, train filename, validation filename, test filename,
    download_dir : str
        The location to download the file to.
    data_home : str
        The location to save the dataset.
    """

    file_path = '{}.zip'.format(download_dir)
    if not Path(file_path).exists():
        urllib.request.urlretrieve(remote.url, file_path)
    _unzip_dataset( file_path, data_home)

def _unzip_dataset(source, destination):
    """Unzip a file from a source location to a destination.
    Parameters
    ----------
    source : str
        The path to the zipped file
    destination : str
        The destination directory to unzip the files to.
    """
    with zipfile.ZipFile(source,'r') as zipObj:
        listOfFileNames = zipObj.namelist()
        # Iterate over the file names
        for fileName in listOfFileNames:
            # Check filename endswith csv
            # print(fileName)
            if fileName.endswith('.txt') and fileName !='fb15k-237/README.txt':
                # Extract a single file from zip
                zipObj.extract(fileName, destination)
    zipObj.close()
    os.remove(source)

def _load_dataset(dataset_metadata, data_home=None):
    """Load a dataset from the details provided.
    DatasetMetadata = namedtuple('DatasetMetadata', ['dataset_name', 'filename', 'url', 'train_name', 'valid_name',
                                                     'test_name', 'train_checksum', 'valid_checksum', 'test_checksum'])
    Parameters
    ----------
    dataset_metadata : DatasetMetadata
        Named tuple containing remote datasets meta information: dataset name, dataset filename,
        url, train filename, validation filename, test filename, train checksum, valid checksum, test checksum.
    data_home : str
        The location to save the dataset to (default: None).
    check_md5hash : boolean
        If True, check the md5hash of the files after they are downloaded (default: False).
    """

    if dataset_metadata.dataset_name is None:
        if dataset_metadata.url is None:
            raise ValueError('The dataset name or url must be provided to load a dataset.')
        dataset_metadata.dataset_name = dataset_metadata.url[dataset_metadata.url.rfind('/') + 1:dataset_metadata
                                                             .url.rfind('.')]
    dataset_path = _fetch_dataset(dataset_metadata, data_home)

    train = load_from_csv(dataset_path, dataset_metadata.train_name)
    valid = load_from_csv(dataset_path, dataset_metadata.valid_name)
    test = load_from_csv(dataset_path, dataset_metadata.test_name)

    return {'train': train, 'valid': valid, 'test': test}

def _fetch_dataset(remote, data_home=None):
    """Get a dataset.
    Gets the directory of a dataset. If the dataset is not found it is downloaded automatically.
    Parameters
    ----------
    remote : DatasetMetadata
        Named tuple containing remote datasets meta information: dataset name, dataset filename,
        url, train filename, validation filename, test filename, train checksum, valid checksum, test checksum.
    data_home : str
        The location to save the dataset to.
    Returns
    ------

    str
        The location of the dataset.
    """
    data_home = _get_data_home(data_home)
    dataset_dir = os.path.join(data_home, remote.dataset_name)
    # print(dataset_dir)
    if not os.path.exists(dataset_dir):
        if remote.url is None:
            msg = 'No dataset at {} and no url provided.'.format(dataset_dir)
            raise Exception(msg)

        _fetch_remote_data(remote, dataset_dir, data_home)
        os.system("cd " + remote.dataset_name + ";cat *.txt > all.txt")
    return dataset_dir

def _load_local_dataset(local,data_home=None):
    data_home = _get_data_home(data_home)
    dataset_dir = os.path.join(data_home, local.dataset_name)
    # print(dataset_dir)
    if not os.path.exists(dataset_dir):
        os.system("cd " + local.dataset_name + ";cat *.txt > all.txt")
    return dataset_dir

def load_from_csv(directory_path, file_name, sep='\t', header=None):

    df = pd.read_csv(os.path.join(directory_path, file_name),
                     sep=sep,
                     header=header,
                     names=None,
                     dtype=str)
    df = df.drop_duplicates()
    return df.values

def _get_data_home(data_home=None):
    """Get to location of the dataset folder to use.
    Automatically determine the dataset folder to use.
    If data_home is provided this location a check is
    performed to see if the path exists and creates one if it does not.
    If data_home is None the AMPLIGRAPH_ENV_NAME dataset is used.
    If AMPLIGRAPH_ENV_NAME is not set the a default environment ``~/ampligraph_datasets`` is used.
    Parameters
    ----------
    data_home : str
       The path to the folder that contains the datasets.
    Returns
    -------
    str
        The path to the dataset directory
    """
    DEFAULT = '/Users/hehuimincheng/Documents/GitHub/KG_brench'
    if data_home is None:
        data_home = os.environ.get(DEFAULT, os.path.join('~', 'ampligraph_datasets'))
    data_home = os.path.expanduser(data_home)
    if not os.path.exists(data_home):
        os.makedirs(data_home)
    return data_home

def _clean_data(X, return_idx=False):
    """
    Clean dataset X by removing unseen entities and relations from valid and test sets.
    Parameters
    ----------
    X: dict
        Dicionary containing the following keys: train, valid, test.
        Each key should contain an ndarray of shape [n, 3].
    return_idx: bool
        Whether to return the indices of the remaining rows in valid and test respectively.
    Returns
    -------
    filtered_X: dict
        Dicionary containing the following keys: train, valid, test.
        Each key contains an ndarray of shape [n, 3].
        Valid and test do not contain entities or relations that are not present in train.
    valid_idx: ndarray
        Indices of the remaining rows of the valid dataset (with respect to the original valid ndarray).
    test_idx: ndarray
        Indices of the remaining rows of the test dataset (with respect to the original test ndarray).
    """
    train = pd.DataFrame(X["train"], columns=['s', 'p', 'o'])
    valid = pd.DataFrame(X["valid"], columns=['s', 'p', 'o'])
    test = pd.DataFrame(X["test"], columns=['s', 'p', 'o'])

    train_ent = np.unique(np.concatenate((train.s, train.o)))
    train_rel = train.p.unique()

    valid_idx = valid.s.isin(train_ent) & valid.o.isin(train_ent) & valid.p.isin(train_rel)
    test_idx = test.s.isin(train_ent) & test.o.isin(train_ent) & test.p.isin(train_rel)

    filtered_valid = valid[valid_idx].values
    filtered_test = test[test_idx].values

    filtered_X = {'train': train.values, 'valid': filtered_valid, 'test': filtered_test}

    if return_idx:
        return filtered_X, valid_idx, test_idx
    else:
        return filtered_X


    # assumption, the new repo added into the list
    #  stop
    #  desired the number of node/ number of repo
    # s' = |W| X (|n|-1)/w XS |n|  (user)