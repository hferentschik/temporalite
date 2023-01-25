package config

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/gs"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
	"gopkg.in/yaml.v2"
)

// Config represents a configuration file for the litestream daemon.
type Config struct {
	// Bind address for serving metrics.
	Addr string `yaml:"addr"`

	// List of databases to manage.
	DBs []*DBConfig `yaml:"dbs"`

	// Subcommand to execute during replication.
	// Litestream will shutdown when subcommand exits.
	Exec string `yaml:"exec"`

	// Global S3 settings
	AccessKeyID     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
}

// DBConfig represents the configuration for a single database.
type DBConfig struct {
	Path                 string         `yaml:"path"`
	MonitorDelayInterval *time.Duration `yaml:"monitor-delay-interval"`
	CheckpointInterval   *time.Duration `yaml:"checkpoint-interval"`
	MinCheckpointPageN   *int           `yaml:"min-checkpoint-page-count"`
	MaxCheckpointPageN   *int           `yaml:"max-checkpoint-page-count"`
	ShadowRetentionN     *int           `yaml:"shadow-retention-count"`

	Replicas []*ReplicaConfig `yaml:"replicas"`
}

// ReplicaConfig represents the configuration for a single replica in a database.
type ReplicaConfig struct {
	Type                   string         `yaml:"type"` // "file", "s3"
	Name                   string         `yaml:"name"` // name of replica, optional.
	Path                   string         `yaml:"path"`
	URL                    string         `yaml:"url"`
	Retention              *time.Duration `yaml:"retention"`
	RetentionCheckInterval *time.Duration `yaml:"retention-check-interval"`
	SyncInterval           *time.Duration `yaml:"sync-interval"`
	SnapshotInterval       *time.Duration `yaml:"snapshot-interval"`
	ValidationInterval     *time.Duration `yaml:"validation-interval"`

	// S3 settings
	AccessKeyID     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
	Endpoint        string `yaml:"endpoint"`
	ForcePathStyle  *bool  `yaml:"force-path-style"`
	SkipVerify      bool   `yaml:"skip-verify"`

	// ABS settings
	AccountName string `yaml:"account-name"`
	AccountKey  string `yaml:"account-key"`

	// SFTP settings
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	KeyPath  string `yaml:"key-path"`
}

// ReplicaType returns the type based on the type field or extracted from the URL.
func (c *ReplicaConfig) ReplicaType() string {
	scheme, _, _, _ := ParseReplicaURL(c.URL)
	if scheme != "" {
		return scheme
	} else if c.Type != "" {
		return c.Type
	}
	return "file"
}

// DefaultConfig returns a new instance of Config with defaults set.
func DefaultConfig() Config {
	return Config{}
}

// DefaultConfigPath returns the default config path.
func DefaultConfigPath() string {
	if v := os.Getenv("LITESTREAM_CONFIG"); v != "" {
		return v
	}
	return "/etc/litestream.yml"
}

// propagateGlobalSettings copies global S3 settings to replica configs.
func (c *Config) propagateGlobalSettings() {
	for _, dbc := range c.DBs {
		for _, rc := range dbc.Replicas {
			if rc.AccessKeyID == "" {
				rc.AccessKeyID = c.AccessKeyID
			}
			if rc.SecretAccessKey == "" {
				rc.SecretAccessKey = c.SecretAccessKey
			}
		}
	}
}

// ReadConfigFile unmarshals config from filename. Expands path if needed.
// If expandEnv is true then environment variables are expanded in the config.
// If filename is blank then the default config path is used.
func ReadConfigFile(filename string, expandEnv bool) (config Config, err error) {
	var filenames []string
	if filename != "" {
		filenames = append(filenames, filename)
	}
	filenames = append(filenames, "./litestream.yml")
	filenames = append(filenames, DefaultConfigPath())

	for _, name := range filenames {
		isDefaultPath := name != filename

		if config, err = readConfigFile(name, expandEnv); os.IsNotExist(err) {
			if isDefaultPath {
				continue
			}
			return config, fmt.Errorf("config file not found: %s", filename)
		} else if err != nil {
			return config, err
		}
		break
	}
	return config, nil
}

// NewReplicaFromConfig instantiates a replica for a DB based on a config.
func NewReplicaFromConfig(c *ReplicaConfig, db *litestream.DB) (_ *litestream.Replica, err error) {
	// Ensure user did not specify URL in path.
	if isURL(c.Path) {
		return nil, fmt.Errorf("replica path cannot be a url, please use the 'url' field instead: %s", c.Path)
	}

	// Build and set client on replica.
	var client litestream.ReplicaClient
	switch typ := c.ReplicaType(); typ {
	case "file":
		if client, err = newFileReplicaClientFromConfig(c); err != nil {
			return nil, err
		}
	case "s3":
		if client, err = newS3ReplicaClientFromConfig(c); err != nil {
			return nil, err
		}
	case "gs":
		if client, err = newGSReplicaClientFromConfig(c); err != nil {
			return nil, err
		}
	case "abs":
		if client, err = newABSReplicaClientFromConfig(c); err != nil {
			return nil, err
		}
	case "sftp":
		if client, err = newSFTPReplicaClientFromConfig(c); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown replica type in config: %q", typ)
	}

	// Build replica.
	r := litestream.NewReplica(db, c.Name, client)
	if v := c.Retention; v != nil {
		r.Retention = *v
	}
	if v := c.RetentionCheckInterval; v != nil {
		r.RetentionCheckInterval = *v
	}
	if v := c.SyncInterval; v != nil {
		r.SyncInterval = *v
	}
	if v := c.SnapshotInterval; v != nil {
		r.SnapshotInterval = *v
	}
	if v := c.ValidationInterval; v != nil {
		r.ValidationInterval = *v
	}

	return r, nil
}

// NewDBFromConfigWithPath instantiates a DB based on a configuration and using a given path.
func NewDBFromConfigWithPath(dbc *DBConfig, path string) (*litestream.DB, error) {
	// Initialize database with given path.
	db := litestream.NewDB(path)

	// Override default database settings if specified in configuration.
	if dbc.MonitorDelayInterval != nil {
		db.MonitorDelayInterval = *dbc.MonitorDelayInterval
	}
	if dbc.CheckpointInterval != nil {
		db.CheckpointInterval = *dbc.CheckpointInterval
	}
	if dbc.MinCheckpointPageN != nil {
		db.MinCheckpointPageN = *dbc.MinCheckpointPageN
	}
	if dbc.MaxCheckpointPageN != nil {
		db.MaxCheckpointPageN = *dbc.MaxCheckpointPageN
	}
	if dbc.ShadowRetentionN != nil {
		db.ShadowRetentionN = *dbc.ShadowRetentionN
	}

	// Instantiate and attach replicas.
	for _, rc := range dbc.Replicas {
		r, err := NewReplicaFromConfig(rc, db)
		if err != nil {
			return nil, err
		}
		db.Replicas = append(db.Replicas, r)
	}

	return db, nil
}

// ParseReplicaURL parses a replica URL.
func ParseReplicaURL(s string) (scheme, host, urlpath string, err error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", "", "", err
	}

	switch u.Scheme {
	case "file":
		scheme, u.Scheme = u.Scheme, ""
		return scheme, "", path.Clean(u.String()), nil

	case "":
		return u.Scheme, u.Host, u.Path, fmt.Errorf("replica url scheme required: %s", s)

	default:
		return u.Scheme, u.Host, strings.TrimPrefix(path.Clean(u.Path), "/"), nil
	}
}

// newFileReplicaClientFromConfig returns a new instance of FileReplicaClient built from config.
func newFileReplicaClientFromConfig(c *ReplicaConfig) (_ *litestream.FileReplicaClient, err error) {
	// Ensure URL & path are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for file replica")
	}

	// Parse path from URL, if specified.
	path := c.Path
	if c.URL != "" {
		if _, _, path, err = ParseReplicaURL(c.URL); err != nil {
			return nil, err
		}
	}

	// Ensure path is set explicitly or derived from URL field.
	if path == "" {
		return nil, fmt.Errorf("file replica path required")
	}

	// Expand home prefix and return absolute path.
	if path, err = expand(path); err != nil {
		return nil, err
	}

	// Instantiate replica and apply time fields, if set.
	return litestream.NewFileReplicaClient(path), nil
}

// newS3ReplicaClientFromConfig returns a new instance of s3.ReplicaClient built from config.
func newS3ReplicaClientFromConfig(c *ReplicaConfig) (_ *s3.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for s3 replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for s3 replica")
	}

	bucket, path := c.Bucket, c.Path
	region, endpoint, skipVerify := c.Region, c.Endpoint, c.SkipVerify

	// Use path style if an endpoint is explicitly set. This works because the
	// only service to not use path style is AWS which does not use an endpoint.
	forcePathStyle := (endpoint != "")
	if v := c.ForcePathStyle; v != nil {
		forcePathStyle = *v
	}

	// Apply settings from URL, if specified.
	if c.URL != "" {
		_, host, upath, err := ParseReplicaURL(c.URL)
		if err != nil {
			return nil, err
		}
		ubucket, uregion, uendpoint, uforcePathStyle := s3.ParseHost(host)

		// Only apply URL parts to field that have not been overridden.
		if path == "" {
			path = upath
		}
		if bucket == "" {
			bucket = ubucket
		}
		if region == "" {
			region = uregion
		}
		if endpoint == "" {
			endpoint = uendpoint
		}
		if !forcePathStyle {
			forcePathStyle = uforcePathStyle
		}
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for s3 replica")
	}

	// Build replica.
	client := s3.NewReplicaClient()
	client.AccessKeyID = c.AccessKeyID
	client.SecretAccessKey = c.SecretAccessKey
	client.Bucket = bucket
	client.Path = path
	client.Region = region
	client.Endpoint = endpoint
	client.ForcePathStyle = forcePathStyle
	client.SkipVerify = skipVerify
	return client, nil
}

// newGSReplicaClientFromConfig returns a new instance of gs.ReplicaClient built from config.
func newGSReplicaClientFromConfig(c *ReplicaConfig) (_ *gs.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for gs replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for gs replica")
	}

	bucket, path := c.Bucket, c.Path

	// Apply settings from URL, if specified.
	if c.URL != "" {
		_, uhost, upath, err := ParseReplicaURL(c.URL)
		if err != nil {
			return nil, err
		}

		// Only apply URL parts to field that have not been overridden.
		if path == "" {
			path = upath
		}
		if bucket == "" {
			bucket = uhost
		}
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for gs replica")
	}

	// Build replica.
	client := gs.NewReplicaClient()
	client.Bucket = bucket
	client.Path = path
	return client, nil
}

// newABSReplicaClientFromConfig returns a new instance of abs.ReplicaClient built from config.
func newABSReplicaClientFromConfig(c *ReplicaConfig) (_ *abs.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for abs replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for abs replica")
	}

	// Build replica.
	client := abs.NewReplicaClient()
	client.AccountName = c.AccountName
	client.AccountKey = c.AccountKey
	client.Bucket = c.Bucket
	client.Path = c.Path
	client.Endpoint = c.Endpoint

	// Apply settings from URL, if specified.
	if c.URL != "" {
		u, err := url.Parse(c.URL)
		if err != nil {
			return nil, err
		}

		if client.AccountName == "" && u.User != nil {
			client.AccountName = u.User.Username()
		}
		if client.Bucket == "" {
			client.Bucket = u.Host
		}
		if client.Path == "" {
			client.Path = strings.TrimPrefix(path.Clean(u.Path), "/")
		}
	}

	// Ensure required settings are set.
	if client.Bucket == "" {
		return nil, fmt.Errorf("bucket required for abs replica")
	}

	return client, nil
}

// newSFTPReplicaClientFromConfig returns a new instance of sftp.ReplicaClient built from config.
func newSFTPReplicaClientFromConfig(c *ReplicaConfig) (_ *sftp.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for sftp replica")
	} else if c.URL != "" && c.Host != "" {
		return nil, fmt.Errorf("cannot specify url & host for sftp replica")
	}

	host, user, password, path := c.Host, c.User, c.Password, c.Path

	// Apply settings from URL, if specified.
	if c.URL != "" {
		u, err := url.Parse(c.URL)
		if err != nil {
			return nil, err
		}

		// Only apply URL parts to field that have not been overridden.
		if host == "" {
			host = u.Host
		}
		if user == "" && u.User != nil {
			user = u.User.Username()
		}
		if password == "" && u.User != nil {
			password, _ = u.User.Password()
		}
		if path == "" {
			path = u.Path
		}
	}

	// Ensure required settings are set.
	if host == "" {
		return nil, fmt.Errorf("host required for sftp replica")
	} else if user == "" {
		return nil, fmt.Errorf("user required for sftp replica")
	}

	// Build replica.
	client := sftp.NewReplicaClient()
	client.Host = host
	client.User = user
	client.Password = password
	client.Path = path
	client.KeyPath = c.KeyPath
	return client, nil
}

// isURL returns true if s can be parsed and has a scheme.
func isURL(s string) bool {
	return regexp.MustCompile(`^\w+:\/\/`).MatchString(s)
}

// readConfigFile unmarshals config from filename. Expands path if needed.
// If expandEnv is true then environment variables are expanded in the config.
func readConfigFile(filename string, expandEnv bool) (_ Config, err error) {
	config := DefaultConfig()

	// Expand filename, if necessary.
	filename, err = expand(filename)
	if err != nil {
		return config, err
	}

	// Read configuration.
	// Do not return an error if using default path and file is missing.
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return config, err
	}

	// Expand environment variables, if enabled.
	if expandEnv {
		buf = []byte(os.ExpandEnv(string(buf)))
	}

	if err := yaml.Unmarshal(buf, &config); err != nil {
		return config, err
	}

	// Normalize paths.
	for _, dbConfig := range config.DBs {
		if dbConfig.Path, err = expand(dbConfig.Path); err != nil {
			return config, err
		}
	}

	// Propage settings from global config to replica configs.
	config.propagateGlobalSettings()

	return config, nil
}

// expand returns an absolute path for s.
func expand(s string) (string, error) {
	// Just expand to absolute path if there is no home directory prefix.
	prefix := "~" + string(os.PathSeparator)
	if s != "~" && !strings.HasPrefix(s, prefix) {
		return filepath.Abs(s)
	}

	// Look up home directory.
	u, err := user.Current()
	if err != nil {
		return "", err
	} else if u.HomeDir == "" {
		return "", fmt.Errorf("cannot expand path %s, no home directory available", s)
	}

	// Return path with tilde replaced by the home directory.
	if s == "~" {
		return u.HomeDir, nil
	}
	return filepath.Join(u.HomeDir, strings.TrimPrefix(s, prefix)), nil
}
