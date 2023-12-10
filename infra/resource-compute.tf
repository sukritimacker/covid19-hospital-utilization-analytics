resource "google_compute_instance" "prefect_vm" {
  name         = "prefect"
  machine_type = "e2-standard-4"
  tags         = ["prefect-vm-rules"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 30
    }
  }

  depends_on = [
    local_file.service_account_json_key,
    google_dataproc_cluster.spark_cluster
  ]

  metadata = {
    ssh-keys = "${split("@", data.google_client_openid_userinfo.me.email)[0]}:${tls_private_key.ssh.public_key_openssh}"
  }

  network_interface {
    network = "default"
    access_config {}
  }

  connection {
    type        = "ssh"
    user        = split("@", data.google_client_openid_userinfo.me.email)[0]
    host        = self.network_interface[0].access_config[0].nat_ip
    private_key = tls_private_key.ssh.private_key_pem
  }

  provisioner "remote-exec" {
    inline = [
      "#!/bin/bash",
      "echo Creating directories",
      "mkdir prefect",
      "mkdir pyspark",
    ]
  }
  
  provisioner "file" {
    source      = "${local.prefect_dir}/"
    destination = "prefect"
  }

  provisioner "file" {
    source      = "${local.scripts_dir}/setup_prefect.sh"
    destination = "setup_prefect.sh"
  }

  provisioner "file" {
    source      = "${local.pyspark_dir}/"
    destination = "pyspark"
  }

  provisioner "remote-exec" {
    inline = [
      "#!/bin/bash",
      "echo export CLUSTER=${google_dataproc_cluster.spark_cluster.name} >> .bashrc",
      "echo export REGION=${var.region} >> .bashrc",
      "echo export DATAPROC_TEMP_BUCKET=${google_storage_bucket.dataproc_temp_bucket.name} >> .bashrc",
      "echo export DATA_LAKE_BUCKET=${google_storage_bucket.data_lake_bucket.name} >> .bashrc",
      "bash -i setup_prefect.sh",
      "gcloud auth activate-service-account ${google_service_account.service_account.email} --key-file=prefect/service_account_creds.json"
    ]
  }
}

resource "google_compute_firewall" "prefect_vm_rules" {
  name          = "prefect-vm-rules"
  network       = "default"
  target_tags   = ["prefect-vm-rules"]
  source_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "tcp"
    ports    = ["4200"]
  }
}
