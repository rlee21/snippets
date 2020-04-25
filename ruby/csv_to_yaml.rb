require 'csv'
require 'yaml'


csv = CSV.read("qna_tags.csv", headers: true)

yaml_hash = csv.inject({}) do | yaml, row |
  specialty_id = row["specialty_id"].to_i

  if row["tag_id_1"] && row["tag_id_2"] && row["tag_id_3"]
    tag_ids = [row["tag_id_1"].to_i, row["tag_id_2"].to_i, row["tag_id_3"].to_i]
  elsif row["tag_id_1"] && row["tag_id_2"] && !row["tag_id_3"]
    tag_ids = [row["tag_id_1"].to_i, row["tag_id_2"].to_i]
  elsif row["tag_id_1"] && !row["tag_id_2"] && !row["tag_id_3"]
    tag_ids = [row["tag_id_1"].to_i]
  end

  yaml[specialty_id] = tag_ids

  yaml
end

File.open("specialty_to_document_tag.yml", "w") { |file| file.write(yaml_hash.to_yaml) }
