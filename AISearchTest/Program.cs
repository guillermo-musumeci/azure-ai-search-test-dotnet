// Azure AI Search Test
// Guillermo Musumeci
// July 4, 2024
// Inspired by https://github.com/Azure/azure-sdk-for-net/tree/main/sdk/search/Azure.Search.Documents/samples

using System;
using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Indexes.Models;
using Azure.Search.Documents.Models;
using System.Net;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace AzureStorageContainerPrivateEndpointExample
{
    #region "Variables"
    public class Product
    {
        [SimpleField(IsKey = true)]
        public string Id { get; set; }

        [SearchableField(IsFilterable = true)]
        public string Name { get; set; }

        [SimpleField(IsSortable = true)]
        public double Price { get; set; }

        public override string ToString() => $"{Id}: {Name} for {Price:C}";
    }
    #endregion

    class Program
    {
        private static string AppSettingsFile = "appsettings.json";

        #region "Create Indexer and Related Resources"
        static void Main(string[] args)
        {
            Console.WriteLine("Azure AI Search Test - Guillermo Musumeci");

            if (File.Exists(AppSettingsFile))
            {
                string dataSourceName = "product-datasource";
                string indexName = "products-index";
                string indexerName = "products-indexer";
                string skillSetName = "product-skillset";
                string containerName = "product";

                // Build configuration
                IConfiguration config = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory) // Ensure the base path is set to the output directory
                .AddJsonFile(AppSettingsFile, optional: false, reloadOnChange: true)
                .Build();

                // Read individual settings
                string? searchName = config["AISearch:Name"];
                string? searchKey = config["AISearch:Key"];

                string? storageAccountName = config["StorageAccount:Name"];
                string? storageAccessKey = config["StorageAccount:Key"];

                string searchServiceEndPoint = $"https://{searchName}.search.windows.net";
                Uri endpoint = new Uri(searchServiceEndPoint);
                AzureKeyCredential credential = new AzureKeyCredential(searchKey);

                CreateIndex(endpoint, credential, indexName);
                CreateStorageDataSource(endpoint, credential, dataSourceName, storageAccountName, storageAccessKey, containerName);
                CreateSkillSet(endpoint, credential, skillSetName);
                CreateIndexer(endpoint, credential, indexerName, indexName, dataSourceName, skillSetName);
            }
            else
            {
                Console.WriteLine($"Error: Cannot Read Configuration File '{AppSettingsFile}");
            }

            Console.WriteLine("Completed!");
            Console.WriteLine("Press any key to continue");
            Console.ReadKey();
        }
        #endregion

        #region "Generate Produc Catalog"
        public static IEnumerable<Product> GenerateCatalog(int count = 100)
        {
            // Adapted from https://weblogs.asp.net/dfindley/Microsoft-Product-Name-Generator
            var prefixes = new[] { null, "Visual", "Compact", "Embedded", "Expression" };
            var products = new[] { null, "Windows", "Office", "SQL", "FoxPro", "BizTalk" };
            var terms = new[] { "Web", "Robotics", "Network", "Testing", "Project", "Small Business", "Team", "Management", "Graphic", "Presentation", "Communication", "Workflow", "Ajax", "XML", "Content", "Source Control" };
            var type = new[] { null, "Client", "Workstation", "Server", "System", "Console", "Shell", "Designer" };
            var suffix = new[] { null, "Express", "Standard", "Professional", "Enterprise", "Ultimate", "Foundation", ".NET", "Framework" };
            var components = new[] { prefixes, products, terms, type, suffix };

            var random = new Random();
            string RandomElement(string[] values) => values[(int)(random.NextDouble() * values.Length)];
            double RandomPrice() => (random.Next(2, 20) * 100.0) / 2.0 - .01;

            for (int i = 1; i <= count; i++)
            {
                yield return new Product
                {
                    Id = i.ToString(),
                    Name = string.Join(" ", components.Select(RandomElement).Where(n => n != null)),
                    Price = RandomPrice()
                };
            }
        }
        #endregion

        #region "Create Index"
        public static bool CreateIndex(Uri endpoint, AzureKeyCredential credential, string indexName)
        {
            try
            {
                SearchIndexClient indexClient = new SearchIndexClient(endpoint, credential);

                try
                {
                    var index = indexClient.GetIndex(indexName);

                    Console.WriteLine($"[INDEX] Index '{indexName}' Exists!");
                }
                catch (RequestFailedException ex) when (ex.Status == 404)
                {
                    indexClient.CreateIndex(new SearchIndex(indexName)
                    {
                        Fields = new FieldBuilder().Build(typeof(Product))
                    });

                    SearchClient searchClient = indexClient.GetSearchClient(indexName);

                    IEnumerable<Product> products = GenerateCatalog(count: 100);
                    searchClient.UploadDocumentsAsync(products);

                    Console.WriteLine($"[INDEX] Index '{indexName}' Created!");
                }

                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[INDEX] Error: Cannot Create Index '{indexName}'. Error: {ex.Message}");

                return false;
            }
        }
        #endregion

        #region "Delete Index"
        public static bool DeleteIndex(Uri endpoint, AzureKeyCredential credential, string indexName)
        {
            SearchIndexClient indexClient = new SearchIndexClient(endpoint, credential);

            Response response = indexClient.DeleteIndex(indexName);
            if (response.Status == 204)
            {
                Console.WriteLine($"[INDEX] Index '{indexName}' deleted successfully.");
                return true;
            }
            else
            {
                Console.WriteLine($"[INDEX] Error: Failed to delete index '{indexName}'. Status: {response.Status}");
                return false;
            }
        }
        #endregion

        #region "Create Data Source for Azure Storage Account"
        public static bool CreateStorageDataSource(Uri endpoint, AzureKeyCredential credential, string dataSourceName, 
            string storageAccountName, string storageAccountKey, string containerName)
        {
            string EndpointUrl = $"https://{storageAccountName}.blob.core.windows.net";
            string storageConnectionString = $"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageAccountKey};EndpointSuffix=core.windows.net";

            // Create a BlobServiceClient object which allows you to interact with the Blob service
            BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);

            // Create a BlobContainerClient object which allows you to interact with the container
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);

            // Create the container if it does not already exist
            try
            {
                containerClient.CreateIfNotExists(PublicAccessType.Blob);
                Console.WriteLine($"[DATA SOURCE] Container '{containerName}' created successfully.");
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"[DATA SOURCE] Error: Cannot Create Container '{containerName}'. Error: {ex.Message}");
            }

            try
            {
                SearchIndexerClient searchIndexerClient = new SearchIndexerClient(endpoint, credential);

                SearchIndexerDataSourceConnection dataSource = new SearchIndexerDataSourceConnection(
                    name: dataSourceName,
                    type: SearchIndexerDataSourceType.AzureBlob,
                    connectionString: storageConnectionString,
                    container: new SearchIndexerDataContainer(containerName)
                );

                var response = searchIndexerClient.CreateOrUpdateDataSourceConnection(dataSource);

                if (response.GetRawResponse().IsError)
                {
                    Console.WriteLine($"[DATA SOURCE] Error: Cannot Create Data Source {dataSourceName}. Error: {response.GetRawResponse().ReasonPhrase}");
                    return false;
                }
                else
                {
                    Console.WriteLine($"[DATA SOURCE] Data Source {dataSourceName} Status: {response.GetRawResponse().ReasonPhrase}");
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DATA SOURCE] Error: Cannot Create Data Source {dataSourceName}. Error: {ex.Message}");
                return false;
            }
        }
        #endregion

        #region "Create SkillSet"
        public static bool CreateSkillSet(Uri endpoint, AzureKeyCredential credential, string skillName)
        {
            try
            {
                SearchIndexerClient indexerClient = new SearchIndexerClient(endpoint, credential);

                Console.WriteLine("[SKILLSET] Creating the skills...");
                OcrSkill ocrSkill = CreateOcrSkill();
                MergeSkill mergeSkill = CreateMergeSkill();
                EntityRecognitionSkill entityRecognitionSkill = CreateEntityRecognitionSkill();
                LanguageDetectionSkill languageDetectionSkill = CreateLanguageDetectionSkill();
                SplitSkill splitSkill = CreateSplitSkill();
                KeyPhraseExtractionSkill keyPhraseExtractionSkill = CreateKeyPhraseExtractionSkill();

                // Create the skillset
                Console.WriteLine("[SKILLSET] Creating or updating the SkillSet...");
                List<SearchIndexerSkill> skills = new List<SearchIndexerSkill>();
                skills.Add(languageDetectionSkill);
                //skills.Add(ocrSkill);
                //skills.Add(mergeSkill);
                //skills.Add(splitSkill);
                //skills.Add(entityRecognitionSkill);
                //skills.Add(keyPhraseExtractionSkill);

                SearchIndexerSkillset skillset = new SearchIndexerSkillset(skillName, skills)
                {
                    // Azure AI services was formerly known as Cognitive Services.
                    // The APIs still use the old name, so we need to create a CognitiveServicesAccountKey object.
                    Description = skillName
                };

                // Create the skillset in your search service.
                // The skillset does not need to be deleted if it was already created
                // since we are using the CreateOrUpdate method
                try
                {
                    indexerClient.CreateOrUpdateSkillset(skillset);
                }
                catch (RequestFailedException ex)
                {
                    Console.WriteLine($"[SKILLSET] Failed to create the skillset. Exception message: {ex.Message}");
                }

                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SKILLSET] Error: Cannot Create Skill Name '{skillName}'. Error: {ex.Message}");

                return false;
            }
        }
        #endregion

        #region "Create Skills"
        private static OcrSkill CreateOcrSkill()
        {
            List<InputFieldMappingEntry> inputMappings = new List<InputFieldMappingEntry>();
            inputMappings.Add(new InputFieldMappingEntry("image")
            {
                Source = "/document/normalized_images/*"
            });

            List<OutputFieldMappingEntry> outputMappings = new List<OutputFieldMappingEntry>();
            outputMappings.Add(new OutputFieldMappingEntry("text")
            {
                TargetName = "text"
            });

            OcrSkill ocrSkill = new OcrSkill(inputMappings, outputMappings)
            {
                Description = "Extract text (plain and structured) from image",
                Context = "/document/normalized_images/*",
                DefaultLanguageCode = OcrSkillLanguage.En,
                ShouldDetectOrientation = true
            };

            return ocrSkill;
        }

        private static MergeSkill CreateMergeSkill()
        {
            List<InputFieldMappingEntry> inputMappings = new List<InputFieldMappingEntry>();
            inputMappings.Add(new InputFieldMappingEntry("text")
            {
                Source = "/document/content"
            });
            inputMappings.Add(new InputFieldMappingEntry("itemsToInsert")
            {
                Source = "/document/normalized_images/*/text"
            });
            inputMappings.Add(new InputFieldMappingEntry("offsets")
            {
                Source = "/document/normalized_images/*/contentOffset"
            });

            List<OutputFieldMappingEntry> outputMappings = new List<OutputFieldMappingEntry>();
            outputMappings.Add(new OutputFieldMappingEntry("mergedText")
            {
                TargetName = "merged_text"
            });

            MergeSkill mergeSkill = new MergeSkill(inputMappings, outputMappings)
            {
                Description = "Create merged_text which includes all the textual representation of each image inserted at the right location in the content field.",
                Context = "/document",
                InsertPreTag = " ",
                InsertPostTag = " "
            };

            return mergeSkill;
        }

        private static LanguageDetectionSkill CreateLanguageDetectionSkill()
        {
            List<InputFieldMappingEntry> inputMappings = new List<InputFieldMappingEntry>();
            inputMappings.Add(new InputFieldMappingEntry("text")
            {
                Source = "/document/merged_text"
            });

            List<OutputFieldMappingEntry> outputMappings = new List<OutputFieldMappingEntry>();
            outputMappings.Add(new OutputFieldMappingEntry("languageCode")
            {
                TargetName = "languageCode"
            });

            LanguageDetectionSkill languageDetectionSkill = new LanguageDetectionSkill(inputMappings, outputMappings)
            {
                Description = "Detect the language used in the document",
                Context = "/document"
            };

            return languageDetectionSkill;
        }
        
        private static SplitSkill CreateSplitSkill()
        {
            List<InputFieldMappingEntry> inputMappings = new List<InputFieldMappingEntry>();
            inputMappings.Add(new InputFieldMappingEntry("text")
            {
                Source = "/document/merged_text"
            });
            inputMappings.Add(new InputFieldMappingEntry("languageCode")
            {
                Source = "/document/languageCode"
            });

            List<OutputFieldMappingEntry> outputMappings = new List<OutputFieldMappingEntry>();
            outputMappings.Add(new OutputFieldMappingEntry("textItems")
            {
                TargetName = "pages",
            });

            SplitSkill splitSkill = new SplitSkill(inputMappings, outputMappings)
            {
                Description = "Split content into pages",
                Context = "/document",
                TextSplitMode = TextSplitMode.Pages,
                MaximumPageLength = 4000,
                DefaultLanguageCode = SplitSkillLanguage.En
            };

            return splitSkill;
        }

        private static EntityRecognitionSkill CreateEntityRecognitionSkill()
        {
            List<InputFieldMappingEntry> inputMappings = new List<InputFieldMappingEntry>();
            inputMappings.Add(new InputFieldMappingEntry("text")
            {
                Source = "/document/pages/*"
            });

            List<OutputFieldMappingEntry> outputMappings = new List<OutputFieldMappingEntry>();
            outputMappings.Add(new OutputFieldMappingEntry("organizations")
            {
                TargetName = "organizations"
            });

            EntityRecognitionSkill entityRecognitionSkill = new EntityRecognitionSkill(inputMappings, outputMappings)
            {
                Description = "Recognize organizations",
                Context = "/document/pages/*",
                DefaultLanguageCode = EntityRecognitionSkillLanguage.En
            };
            entityRecognitionSkill.Categories.Add(EntityCategory.Organization);

            return entityRecognitionSkill;
        }

        private static KeyPhraseExtractionSkill CreateKeyPhraseExtractionSkill()
        {
            List<InputFieldMappingEntry> inputMappings = new List<InputFieldMappingEntry>();
            inputMappings.Add(new InputFieldMappingEntry("text")
            {
                Source = "/document/pages/*"
            });
            inputMappings.Add(new InputFieldMappingEntry("languageCode")
            {
                Source = "/document/languageCode"
            });

            List<OutputFieldMappingEntry> outputMappings = new List<OutputFieldMappingEntry>();
            outputMappings.Add(new OutputFieldMappingEntry("keyPhrases")
            {
                TargetName = "keyPhrases"
            });

            KeyPhraseExtractionSkill keyPhraseExtractionSkill = new KeyPhraseExtractionSkill(inputMappings, outputMappings)
            {
                Description = "Extract the key phrases",
                Context = "/document/pages/*",
                DefaultLanguageCode = KeyPhraseExtractionSkillLanguage.En
            };

            return keyPhraseExtractionSkill;
        }
        #endregion

        #region "Create Search Indexer"
        private static SearchIndexer CreateIndexer(Uri endpoint, AzureKeyCredential credential, string indexerName, string indexName, string dataSourceName, string skillSetName)
        {
            SearchIndexerClient indexerClient = new SearchIndexerClient(endpoint, credential);

            IndexingParameters indexingParameters = new IndexingParameters()
            {
                MaxFailedItems = -1,
                MaxFailedItemsPerBatch = -1,
            };

            SearchIndexer indexer = new SearchIndexer(indexerName, dataSourceName, indexName)
            {
                Description = indexerName,
                SkillsetName = skillSetName,
                Parameters = indexingParameters
            };

            try
            {
                indexerClient.GetIndexer(indexerName);
                indexerClient.DeleteIndexer(indexerName);

                Console.WriteLine($"[INDEXER] Creating or updating the Indexer '{indexerName}'...");
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                Console.WriteLine($"[INDEXER] Cannot Create the Indexer '{indexerName}'...");
            }

            try
            {
                indexerClient.CreateIndexer(indexer);
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"[INDEXER] Failed to create the Indexer '{indexerName}'. Exception Message: {ex.Message}");
            }

            return indexer;
        }
        #endregion
    }
}