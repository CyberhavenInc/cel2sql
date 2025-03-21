package test

import (
	"time"

	"cloud.google.com/go/bigquery"
)

func NewTrigramsTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{
		Name:        "",
		Location:    "US",
		Description: "",
		Schema: bigquery.Schema{
			&bigquery.FieldSchema{
				Name:        "ngram",
				Description: "",
				Repeated:    false,
				Required:    false,
				Type:        "STRING",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "first",
				Description: "",
				Repeated:    false,
				Required:    false,
				Type:        "STRING",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "second",
				Description: "",
				Repeated:    false,
				Required:    false,
				Type:        "STRING",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "third",
				Description: "",
				Repeated:    false,
				Required:    false,
				Type:        "STRING",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "fourth",
				Description: "",
				Repeated:    false,
				Required:    false,
				Type:        "STRING",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "fifth",
				Description: "",
				Repeated:    false,
				Required:    false,
				Type:        "STRING",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "cell",
				Description: "",
				Repeated:    true,
				Required:    false,
				Type:        "RECORD",
				PolicyTags:  nil,
				Schema: bigquery.Schema{
					&bigquery.FieldSchema{
						Name:        "value",
						Description: "",
						Repeated:    true,
						Required:    false,
						Type:        "STRING",
						PolicyTags:  nil,
						Schema:      nil,
					},
					&bigquery.FieldSchema{
						Name:        "volume_count",
						Description: "",
						Repeated:    false,
						Required:    false,
						Type:        "INTEGER",
						PolicyTags:  nil,
						Schema:      nil,
					},
					&bigquery.FieldSchema{
						Name:        "volume_fraction",
						Description: "",
						Repeated:    false,
						Required:    false,
						Type:        "FLOAT",
						PolicyTags:  nil,
						Schema:      nil,
					},
					&bigquery.FieldSchema{
						Name:        "page_count",
						Description: "",
						Repeated:    false,
						Required:    false,
						Type:        "INTEGER",
						PolicyTags:  nil,
						Schema:      nil,
					},
					&bigquery.FieldSchema{
						Name:        "match_count",
						Description: "",
						Repeated:    false,
						Required:    false,
						Type:        "INTEGER",
						PolicyTags:  nil,
						Schema:      nil,
					},
					&bigquery.FieldSchema{
						Name:        "sample",
						Description: "",
						Repeated:    true,
						Required:    false,
						Type:        "RECORD",
						PolicyTags:  nil,
						Schema: bigquery.Schema{
							&bigquery.FieldSchema{
								Name:        "id",
								Description: "",
								Repeated:    false,
								Required:    false,
								Type:        "STRING",
								PolicyTags:  nil,
								Schema:      nil,
							},
							&bigquery.FieldSchema{
								Name:        "text",
								Description: "",
								Repeated:    false,
								Required:    false,
								Type:        "STRING",
								PolicyTags:  nil,
								Schema:      nil,
							},
							&bigquery.FieldSchema{
								Name:        "title",
								Description: "",
								Repeated:    false,
								Required:    false,
								Type:        "STRING",
								PolicyTags:  nil,
								Schema:      nil,
							},
							&bigquery.FieldSchema{
								Name:        "subtitle",
								Description: "",
								Repeated:    false,
								Required:    false,
								Type:        "STRING",
								PolicyTags:  nil,
								Schema:      nil,
							},
							&bigquery.FieldSchema{
								Name:        "authors",
								Description: "",
								Repeated:    false,
								Required:    false,
								Type:        "STRING",
								PolicyTags:  nil,
								Schema:      nil,
							},
							&bigquery.FieldSchema{
								Name:        "url",
								Description: "",
								Repeated:    false,
								Required:    false,
								Type:        "STRING",
								PolicyTags:  nil,
								Schema:      nil,
							},
						},
					},
				},
			},
		},
		MaterializedView:       nil,
		ViewQuery:              "",
		UseLegacySQL:           false,
		UseStandardSQL:         false,
		TimePartitioning:       nil,
		RangePartitioning:      nil,
		RequirePartitionFilter: false,
		Clustering:             nil,
		ExpirationTime:         time.Now(),
		Labels:                 nil,
		ExternalDataConfig:     nil,
		EncryptionConfig:       nil,
		FullID:                 "bigquery-public-data:samples.trigrams",
		Type:                   "TABLE",
		CreationTime:           time.Now(),
		LastModifiedTime:       time.Now(),
		NumBytes:               277168458677,
		NumLongTermBytes:       277168458677,
		NumRows:                0x40e6235,
		StreamingBuffer:        nil,
		ETag:                   "coBH0z/sk1ardM9lC1MCMw==",
	}
}

func NewWikipediaTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{
		Name:        "",
		Location:    "US",
		Description: "",
		Schema: bigquery.Schema{
			&bigquery.FieldSchema{
				Name:        "title",
				Description: "The title of the page, as displayed on the page (not in the URL). Always starts with a capital letter and may begin with a namespace (e.g. \"Talk:\", \"User:\", \"User Talk:\", ... )",
				Repeated:    false,
				Required:    true,
				Type:        "STRING",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "id",
				Description: "A unique ID for the article that was revised. These correspond to the order in which articles were created, except for the first several thousand IDs, which are issued in alphabetical order.",
				Repeated:    false,
				Required:    false,
				Type:        "INTEGER",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "language",
				Description: "Empty in the current dataset.",
				Repeated:    false,
				Required:    true,
				Type:        "STRING",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "wp_namespace",
				Description: "Wikipedia segments its pages into namespaces (e.g. \"Talk\", \"User\", etc.)\n\nMEDIA = 202; // =-2 in WP XML, but these values must be >0\nSPECIAL = 201; // =-1 in WP XML, but these values must be >0\nMAIN = 0;\nTALK = 1;\nUSER = 2;\nUSER_TALK = 3;\nWIKIPEDIA = 4;\nWIKIPEDIA_TALK = 5;\nIMAGE  = 6;  // Has since been renamed to \"File\" in WP XML.\nIMAGE_TALK = 7;  // Equivalent to \"File talk\".\nMEDIAWIKI = 8;\nMEDIAWIKI_TALK = 9;\nTEMPLATE = 10;\nTEMPLATE_TALK = 11;\nHELP = 12;\nHELP_TALK = 13;\nCATEGORY = 14;\nCATEGORY_TALK = 15;\nPORTAL = 100;\nPORTAL_TALK = 101;\nWIKIPROJECT = 102;\nWIKIPROJECT_TALK = 103;\nREFERENCE = 104;\nREFERENCE_TALK = 105;\nBOOK = 108;\nBOOK_TALK = 109;",
				Repeated:    false,
				Required:    true,
				Type:        "INTEGER",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "is_redirect",
				Description: "Versions later than ca. 200908 may have a redirection marker in the XML.",
				Repeated:    false,
				Required:    false,
				Type:        "BOOLEAN",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "revision_id",
				Description: "These are unique across all revisions to all pages in a particular language and increase with time. Sorting the revisions to a page by revision_id will yield them in chronological order.",
				Repeated:    false,
				Required:    false,
				Type:        "INTEGER",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "contributor_ip",
				Description: "Typically, either _ip or (_id and _username) will be set. IP information is unavailable for edits from registered accounts. A (very) small fraction of edits have neither _ip or (_id and _username). They show up on Wikipedia as \"(Username or IP removed)\".",
				Repeated:    false,
				Required:    false,
				Type:        "STRING",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "contributor_id",
				Description: "Typically, either (_id and _username) or _ip will be set. A (very) small fraction of edits have neither _ip or (_id and _username). They show up on Wikipedia as \"(Username or IP removed)\".",
				Repeated:    false,
				Required:    false,
				Type:        "INTEGER",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "contributor_username",
				Description: "Typically, either (_id and _username) or _ip will be set. A (very) small fraction of edits have neither _ip or (_id and _username). They show up on Wikipedia as \"(Username or IP removed)\".",
				Repeated:    false,
				Required:    false,
				Type:        "STRING",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "timestamp",
				Description: "last edit to the page",
				Repeated:    false,
				Required:    false,
				Type:        "TIMESTAMP",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "is_minor",
				Description: "Corresponds to the \"Minor Edit\" checkbox on Wikipedia's edit page.",
				Repeated:    false,
				Required:    false,
				Type:        "BOOLEAN",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "is_bot",
				Description: "A special flag that some of Wikipedia's more active bots voluntarily set.",
				Repeated:    false,
				Required:    false,
				Type:        "BOOLEAN",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "reversion_id",
				Description: "If this edit is a reversion to a previous edit, this field records the revision_id that was reverted to. If the same article text occurred multiple times, then this will point to the earliest revision. Only revisions with greater than fifty characters are considered for this field. This is to avoid labeling multiple blankings as reversions.",
				Repeated:    false,
				Required:    false,
				Type:        "INTEGER",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "comment",
				Description: "Optional user-supplied description of the edit. Section edits are, by default, prefixed with \"/* Section Name */ \".",
				Repeated:    false,
				Required:    false,
				Type:        "STRING",
				PolicyTags:  nil,
				Schema:      nil,
			},
			&bigquery.FieldSchema{
				Name:        "num_characters",
				Description: "The length of the article after the revision was applied.",
				Repeated:    false,
				Required:    true,
				Type:        "INTEGER",
				PolicyTags:  nil,
				Schema:      nil,
			},
		},
		MaterializedView:       nil,
		ViewQuery:              "",
		UseLegacySQL:           false,
		UseStandardSQL:         false,
		TimePartitioning:       nil,
		RangePartitioning:      nil,
		RequirePartitionFilter: false,
		Clustering:             nil,
		ExpirationTime:         time.Time{},
		Labels:                 nil,
		ExternalDataConfig:     nil,
		EncryptionConfig:       nil,
		FullID:                 "bigquery-public-data:samples.wikipedia",
		Type:                   "TABLE",
		CreationTime:           time.Now(),
		LastModifiedTime:       time.Now(),
		NumBytes:               38324173849,
		NumLongTermBytes:       38324173849,
		NumRows:                0x12b429ab,
		StreamingBuffer:        nil,
		ETag:                   "banEhEDm4Cu2wGJcfhspUg==",
	}
}
